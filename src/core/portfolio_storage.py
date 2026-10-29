"""Atomic portfolio publication and explicit child ownership."""
import json
from datetime import datetime, timedelta

from .portfolio_projection import project


def display_end(exclusive_end):
    """Calendar period labels include the last day, while facts keep [start, end)."""
    return (datetime.fromisoformat(exclusive_end.replace('Z', '+00:00')) - timedelta(days=1)).date().isoformat()


def create_schema(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id TEXT PRIMARY KEY,
            set_id INTEGER NOT NULL UNIQUE REFERENCES study_sets(id) ON DELETE CASCADE,
            facts_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS portfolio_accounts (
            account_id TEXT PRIMARY KEY,
            portfolio_id TEXT NOT NULL REFERENCES portfolios(portfolio_id) ON DELETE CASCADE,
            period_number INTEGER NOT NULL,
            instrument_id TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('funded','comparison')),
            opening_capital REAL NOT NULL,
            status TEXT NOT NULL,
            study_id TEXT NOT NULL UNIQUE REFERENCES studies(study_id) ON DELETE RESTRICT
        );
        CREATE VIEW IF NOT EXISTS ordinary_studies AS
            SELECT * FROM studies WHERE study_id NOT IN (SELECT study_id FROM portfolio_accounts);
        CREATE TRIGGER IF NOT EXISTS portfolio_owned_membership_insert
        BEFORE INSERT ON study_set_members
        WHEN EXISTS (SELECT 1 FROM portfolio_accounts WHERE study_id = NEW.study_id)
          OR EXISTS (SELECT 1 FROM portfolios WHERE set_id = NEW.set_id)
        BEGIN SELECT RAISE(ABORT, 'Portfolio membership is owned; ordinary set edits are not supported.'); END;
        CREATE TRIGGER IF NOT EXISTS portfolio_owned_membership_update
        BEFORE UPDATE ON study_set_members
        WHEN EXISTS (SELECT 1 FROM portfolio_accounts WHERE study_id = NEW.study_id)
          OR EXISTS (SELECT 1 FROM portfolios WHERE set_id = NEW.set_id)
        BEGIN SELECT RAISE(ABORT, 'Portfolio membership is owned; ordinary set edits are not supported.'); END;
    """)


def publish_portfolio(record, children, *, started, queue_number=None, cancelled=lambda: False):
    from . import storage
    from .data_input import timeframe_label
    from strategies import get_strategy_config
    strategy = get_strategy_config(record["strategy"])["name"]
    selector = record["selector"]
    name = f"PF {strategy} · {timeframe_label(record['timeframeMinutes'])} · X Top {selector['topN']} / {selector['rebalanceMonths']}m"
    if record["wfa"]:
        name += f" · WFA {record['wfa']['is_period_months']}m/{record['wfa']['oos_period_months']}m"
    name += " · Fixed" if record["request"].get("parameterMode") == "fixed" else " · Grid"
    if queue_number is not None:
        name = f"#{queue_number} · {name}"
    with storage.get_db_connection() as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            suffix, actual = 1, name
            while conn.execute("SELECT 1 FROM study_sets WHERE name = ? COLLATE NOCASE", (actual,)).fetchone():
                suffix += 1
                actual = f"{name} ({suffix})"
            order = conn.execute("SELECT COALESCE(MAX(sort_order),0)+1 FROM study_sets").fetchone()[0]
            cursor = conn.execute("INSERT INTO study_sets(name,sort_order) VALUES (?,?)", (actual, order))
            record.update(setId=cursor.lastrowid, name=actual)
            conn.execute("INSERT INTO portfolios VALUES (?,?,?)", (record["id"], record["setId"], "{}"))
            for account, kind, args in children:
                if cancelled():
                    raise ValueError("Portfolio cancelled before publication.")
                if kind == "wfa":
                    wf_result, template = args
                    study = storage.save_wfa_study_to_db(wf_result, template, "", started, connection=conn)
                else:
                    config, settings, summary, results = args
                    study = storage.save_grid_study_to_db(config=config, grid_settings=settings, grid_summary=summary,
                        trial_results=results, csv_file_path="", start_time=started, connection=conn)
                account["studyId"] = study
                conn.execute("INSERT INTO portfolio_accounts VALUES (?,?,?,?,?,?,?,?)",
                    (account["id"], record["id"], account["period"], account["instrument"], account["role"],
                     account["opening"], account["status"], study))
            conn.execute("UPDATE portfolios SET facts_json=? WHERE portfolio_id=?",
                (json.dumps(record, allow_nan=False, default=lambda x: x.isoformat()), record["id"]))
            if cancelled():
                raise ValueError("Portfolio cancelled before publication.")
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def load_portfolio(identifier, *, conn=None):
    if conn is None:
        from .storage import get_db_connection
        with get_db_connection() as connection:
            return load_portfolio(identifier, conn=connection)
    row = conn.execute("SELECT facts_json FROM portfolios WHERE portfolio_id=?", (identifier,)).fetchone()
    if row is None:
        raise ValueError("Portfolio not found.")
    return json.loads(row[0])


def enrich_sets(conn, sets):
    by_set = {r["set_id"]: json.loads(r["facts_json"]) for r in conn.execute("SELECT * FROM portfolios")}
    for item in sets:
        record = by_set.get(item["id"])
        if record is None:
            continue
        item.update(kind="portfolio", portfolio_id=record["id"], metrics={**record["metrics"], "portfolio": True}, selector=record["selector"],
                    period_count=len(record["periods"]), status=record["status"],
                    periods=[{"id": p["id"], "start": p["start"], "end": p["end"], "displayEnd": display_end(p["end"]), "status": p.get("status"),
                              "funded_count": sum(a["role"] == "funded" and bool(a.get("path")) for a in p["accounts"]),
                              "metrics": project(record, {"periods": [p["id"]]})} for p in record["periods"]])
    return sets


def delete_owned_set(conn, set_id):
    row = conn.execute("SELECT portfolio_id FROM portfolios WHERE set_id=?", (set_id,)).fetchone()
    if row is None:
        return
    ids = [r[0] for r in conn.execute("SELECT study_id FROM portfolio_accounts WHERE portfolio_id=?", (row[0],))]
    conn.execute("DELETE FROM portfolio_accounts WHERE portfolio_id=?", (row[0],))
    conn.executemany("DELETE FROM studies WHERE study_id=?", [(study,) for study in ids])
    conn.execute("DELETE FROM portfolios WHERE portfolio_id=?", (row[0],))


def account_context(study_id):
    from .storage import get_db_connection
    with get_db_connection() as conn:
        row = conn.execute("SELECT * FROM portfolio_accounts WHERE study_id=?", (study_id,)).fetchone()
        if row is None:
            return None
        record = load_portfolio(row["portfolio_id"], conn=conn)
        account = next(a for p in record["periods"] for a in p["accounts"] if a["id"] == row["account_id"])
        return {"portfolioId": record["id"], "setId": record["setId"], "name": record["name"],
                "account": account, "metrics": project(record, {"accounts": [account["id"]]})}
