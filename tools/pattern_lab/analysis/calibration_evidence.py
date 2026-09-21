"""Admission of monthly research evidence, independent of generation and memory APIs.

Format 1 is the delivered archive; format 2 adds plan/source/primary/replay identity.
The decision policy is versioned separately from the unchanged numerical method.
"""
import gzip
import hashlib
import json
import math
import re

import numpy as np

from ..study.contracts import semantic_digest
from .estimator import _holm

EVIDENCE_SCHEMA_VERSION = 2
PLAN_VERSION = 1
DECISION_POLICY_VERSION = 2
REPLAY_IDS = [20000, 20001]
SHA256 = re.compile(r"[0-9a-fA-F]{64}")


def load_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result
    def invalid(value):
        raise ValueError(f"non-finite JSON value {value}")
    def floating(value):
        result = float(value)
        if not math.isfinite(result):
            invalid(value)
        return result
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid, parse_float=floating)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def same(left, right):
    """Exact semantic comparison, including Boolean versus integer identity."""
    return semantic_digest(left) == semantic_digest(right)


def manifest_problems(saved, expected, scenario_names, research_modules):
    problems = []
    version = saved.get("schema_version")
    if type(version) is not int or version not in (1, EVIDENCE_SCHEMA_VERSION):
        problems.append("unknown evidence schema_version")
    if not same(saved.get("plan_version", 1 if version == 1 else None), PLAN_VERSION):
        problems.append("unknown or missing plan_version")
    if saved.get("manifest_digest") != semantic_digest({k: v for k, v in saved.items() if k != "manifest_digest"}):
        problems.append("the saved manifest digest does not match its own content")
    for key in ("method", "scope"):
        if saved.get(key) != expected[key]:
            problems.append(f"manifest {key} differs from plan 1")
    # Only semantic fields: neither producer hashes nor mutable explanatory prose.
    fields = {
        "family": ("record_primary", "candle_primary_member_id", "alpha", "confidence_level"),
        "seeds": ("master_seed", "statistical_range", "pilot_ids"),
        "decision": ("error_envelope", "min_primary_availability", "rates", "impossible_errors", "impossible_unavailable"),
        "budget": ("wall_clock_seconds", "rss_ceiling_bytes", "headroom_required_bytes", "headroom_reserve_bytes", "pilot_attempts"),
    }
    for section, keys in fields.items():
        actual = saved.get(section, {})
        if not isinstance(actual, dict) or not same({key: actual.get(key) for key in keys},
                                                    {key: expected[section][key] for key in keys}):
            problems.append(f"manifest {section} settings differ from plan 1")
    validity = saved.get("formula", {}).get("validity", {})
    keys = ("min_informative_months", "positive_deletion_denominator", "finite_inputs_and_results", "nondegenerate_contrast")
    if not same({k: validity.get(k) for k in keys}, {k: expected["formula"]["validity"][k] for k in keys}):
        problems.append("manifest mathematical validity settings differ from plan 1")
    matrix = saved.get("matrix", {})
    for group in ("main", "supplementary"):
        entries = matrix.get(group)
        reference = expected["matrix"][group]
        keys = ("order", "fixture_id", "name", "kind", "attempts", "source", "scenario_name", "padded_days", "required")
        if not isinstance(entries, list) or not same(
            [{k: item.get(k) for k in keys} for item in entries],
            [{k: item[k] for k in keys} for item in reference],
        ):
            problems.append(f"manifest {group} matrix differs from frozen plan 1")
    if not same(matrix.get("max_main_attempts"), 16000):
        problems.append("manifest max_main_attempts differs from plan 1")
    generators = saved.get("generators", {})
    for kind in ("record_contract", "candle_contract"):
        actual = generators.get(kind, {})
        reference = expected["generators"][kind]
        # Numerical settings and explicit identities are the semantic contract.
        excluded = {"implementation", "generator_digest", "scenarios", "seed_rule", "draw_order",
                    "ar_initialization", "signal_initialization", "months", "outcome_model",
                    "confounded_probability_month_ownership", "bounded_law_disclosure",
                    "common_signal_disclosure", "null_argument", "price_law", "return_law", "volume_law"}
        keys = [key for key in reference if key not in excluded]
        if not same({k: actual.get(k) for k in keys}, {k: reference[k] for k in keys}):
            problems.append(f"manifest {kind} settings differ from plan 1")
        if kind == "record_contract":
            if actual.get("generator_digest") != semantic_digest({k: v for k, v in actual.items() if k != "generator_digest"}):
                problems.append("record generator digest does not match its own contract")
            for name in scenario_names:
                found = [s for s in actual.get("scenarios", []) if s.get("name") == name]
                wanted = [s for s in reference["scenarios"] if s["name"] == name]
                def config(items):
                    return [{k: v for k, v in item.items() if k not in ("note", "caveat")} for item in items]
                if len(found) != 1 or not same(config(found), config(wanted)):
                    problems.append(f"record generator required fixture {name} missing, duplicated or changed")
    if version == EVIDENCE_SCHEMA_VERSION:
        if not same(saved.get("decision_policy_version"), DECISION_POLICY_VERSION):
            problems.append("new manifest decision_policy_version is missing or unknown")
        attribution = saved.get("research_implementation", {})
        if not isinstance(attribution, dict) or any(
            not isinstance(attribution.get(name), str) or not SHA256.fullmatch(attribution[name])
            for name in research_modules
        ):
            problems.append("new manifest lacks required research source hashes")
    return problems


def record_bytes(root, label):
    if not isinstance(label, str) or re.fullmatch(r"[A-Za-z0-9_]+", label) is None:
        raise ValueError("unsafe record label")
    plain = root / "records" / f"{label}.json"
    packed = root / "records" / f"{label}.json.gz"
    if plain.exists() and packed.exists():
        raise ValueError(f"{label}: ambiguous plain and gzip record files")
    if plain.is_file():
        return plain.read_bytes()
    if packed.is_file():
        return gzip.decompress(packed.read_bytes())
    return None


def checksums(root, labels):
    path = root / "records.sha256"
    if not path.exists():
        return None
    entries = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-fA-F]{64})  ([A-Za-z0-9_]+)\.json", line)
        if match is None:
            raise ValueError("records.sha256: malformed checksum line")
        digest, label = match.groups()
        if label not in labels or label in entries:
            raise ValueError(f"records.sha256: unknown or duplicate label {label!r}")
        entries[label] = digest.lower()
    return entries


def validate_rows(rows, *, fixture, member_ids, primary_id, new_format):
    """Admit the finite/null evidence used by the gate before counting any row."""
    if not isinstance(rows, list):
        raise ValueError("rows must be a list")
    if len(rows) > fixture.attempts:
        raise ValueError("record count exceeds declared attempts")
    ids = [row["id"] for row in rows]
    if any(type(value) is not int for value in ids):
        raise ValueError("repetition IDs must be integers, not booleans")
    if len(set(ids)) != len(ids) or ids != sorted(ids) or any(not 20000 <= value < 20000 + fixture.attempts for value in ids):
        raise ValueError("duplicate, reordered or out-of-range repetition IDs")
    size = len(member_ids)
    primary = member_ids.index(primary_id)
    for row in rows:
        try:
            if row["member_ids"] != member_ids or type(row["family_size"]) is not int or row["family_size"] != size:
                raise ValueError("wrong ordered member identities or family_size")
            if new_format and row.get("primary_member_id") != primary_id:
                raise ValueError("wrong or missing primary_member_id")
            for key in ("member_available", "member_p_raw", "member_p_holm", "member_reject_raw", "member_reject_holm"):
                if not isinstance(row[key], list) or len(row[key]) != size:
                    raise ValueError(f"ragged {key}")
            available = row["member_available"]
            if any(type(value) is not bool for value in available):
                raise ValueError("member availability must be Boolean")
            internal = []
            for index, enabled in enumerate(available):
                raw, adjusted = row["member_p_raw"][index], row["member_p_holm"][index]
                for p, flag in ((raw, row["member_reject_raw"][index]), (adjusted, row["member_reject_holm"][index])):
                    if enabled:
                        if not finite(p) or not 0 <= p <= 1 or type(flag) is not bool or flag != (p <= .05):
                            raise ValueError("member p-value / rejection flag contradicts availability or numeric evidence")
                    elif p is not None or flag is not None:
                        raise ValueError("unavailable member publishes inference")
                internal.append(raw if enabled else 1.)
            adjusted = _holm(np.asarray(internal, dtype=float), member_ids)
            for index, enabled in enumerate(available):
                if enabled and not math.isclose(row["member_p_holm"][index], float(adjusted[index]), rel_tol=1e-12, abs_tol=1e-15):
                    raise ValueError("saved Holm p-values contradict the declared family")
            for field, vector in (("available", "member_available"), ("p_raw", "member_p_raw"),
                                  ("p_holm", "member_p_holm"), ("reject_raw", "member_reject_raw"),
                                  ("reject_holm", "member_reject_holm")):
                if not same(row[field], row[vector][primary]):
                    raise ValueError(f"primary {field} contradicts its member")
            if type(row["family_available"]) is not int or row["family_available"] != sum(available):
                raise ValueError("family_available contradicts members")
            family_rejection = any(flag is True for flag in row["member_reject_holm"])
            if type(row["family_any_rejection"]) is not bool or row["family_any_rejection"] != family_rejection:
                raise ValueError("family rejection flag contradicts members")
            for key in ("reasons", "inherited_reasons"):
                if not isinstance(row[key], list) or any(not isinstance(s, str) or not s for s in row[key]):
                    raise ValueError(f"invalid {key}")
            if row["available"]:
                if row["reasons"] or row["inherited_reasons"]:
                    raise ValueError("available primary has refusal reasons")
                for key in ("lift", "standard_error", "interval_lower", "interval_upper", "interval_width"):
                    if not finite(row[key]):
                        raise ValueError(f"available primary has non-finite/null {key}")
                lower, upper = row["interval_lower"], row["interval_upper"]
                if row["standard_error"] <= 0 or lower > upper or not lower <= row["lift"] <= upper:
                    raise ValueError("invalid primary interval or standard error")
                if not math.isclose(row["interval_width"], upper - lower, rel_tol=1e-12, abs_tol=1e-15):
                    raise ValueError("interval width contradicts bounds")
                scale = max(abs(row["lift"]), row["standard_error"])
                if not math.isclose((lower + upper) / 2, row["lift"], rel_tol=1e-12, abs_tol=1e-12 * scale):
                    raise ValueError("interval is not centred on the reported primary lift")
                excludes_zero = not lower <= 0 <= upper
                boundary_roundoff = (min(abs(lower), abs(upper)) <= 1e-12 * scale
                                     and abs(row["p_raw"] - .05) <= 1e-12)
                if row["reject_raw"] != excludes_zero and not boundary_roundoff:
                    raise ValueError("primary p-value and interval contradict their zero-null inversion")
                if type(row["noncoverage"]) is not bool or row["noncoverage"] != (not lower <= fixture.truth <= upper):
                    raise ValueError("noncoverage flag contradicts interval / fixture truth")
                if type(row["informative_months"]) is not int or row["informative_months"] < 2:
                    raise ValueError("invalid informative_months")
                if type(row["degrees_of_freedom"]) is not int or row["degrees_of_freedom"] != row["informative_months"] - 1:
                    raise ValueError("invalid degrees_of_freedom")
            else:
                if not row["reasons"]:
                    raise ValueError("unavailable primary lacks reasons")
                for key in ("standard_error", "interval_lower", "interval_upper", "interval_width", "noncoverage"):
                    if row[key] is not None:
                        raise ValueError(f"unavailable primary publishes {key}")
                if row["lift"] is not None and not finite(row["lift"]):
                    raise ValueError("non-finite descriptive lift")
        except (KeyError, TypeError, ValueError, IndexError) as error:
            raise ValueError(f"repetition {row.get('id')}: {error}") from error


def replay_problems(replays, families, *, new_format):
    missing, problems = [], []
    if replays is None:
        return ["the bounded replay and adapter checks were not run"], []
    if not isinstance(replays, dict):
        return [], ["replays must be a mapping"]
    if set(replays) - set(families):
        problems.append("unknown replay names")
    for name, member_ids in families.items():
        replay = replays.get(name)
        if replay is None:
            missing.append(f"{name}: required replay missing")
            continue
        try:
            if not same(replay["repetitions"], REPLAY_IDS):
                raise ValueError("wrong replay IDs")
            if "scenario" in replay and replay["scenario"] != "null_dependent_t5":
                raise ValueError("wrong recorded scenario identity")
            comparisons = replay["comparisons"]
            if not isinstance(comparisons, list) or not same([r["repetition"] for r in comparisons], REPLAY_IDS):
                raise ValueError("missing, duplicate or reordered replay comparisons")
            for row in [*comparisons, replay]:
                delta = row["max_absolute_difference"]
                if not finite(delta) or not 0 <= delta <= 1e-12 or row["mismatched"] != []:
                    raise ValueError("numerical agreement evidence fails")
            if replay["agrees"] is not True:
                raise ValueError("agreement must be Boolean true")
            if replay["max_absolute_difference"] != max(r["max_absolute_difference"] for r in comparisons):
                raise ValueError("aggregate difference contradicts comparisons")
            if new_format:
                for row in comparisons:
                    for side in ("expected", "left", "right"):
                        if row.get(f"{side}_member_ids") != member_ids or not same(row.get(f"{side}_member_count"), len(member_ids)):
                            raise ValueError("missing or incorrect new-format replay family structure")
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as error:
            problems.append(f"{name}: {error}")
    return missing, problems


def run_problems(run, manifest, labels):
    missing, problems = [], []
    if run is None:
        return ["run.json is missing"], []
    for key in ("method", "schema_version", "manifest_digest"):
        if not same(run.get(key), manifest.get(key)):
            problems.append(f"run {key} contradicts manifest")
    if manifest.get("schema_version") == EVIDENCE_SCHEMA_VERSION:
        for key, expected in (("plan_version", PLAN_VERSION), ("decision_policy_version", DECISION_POLICY_VERSION)):
            if not same(run.get(key), expected):
                problems.append(f"run {key} missing or inconsistent")
    elif "plan_version" in run and not same(run["plan_version"], PLAN_VERSION):
        problems.append("legacy run plan_version contradicts plan 1")
    planned = run.get("planned_fixtures")
    if not isinstance(planned, list) or any(not isinstance(s, str) for s in planned):
        problems.append("run planned_fixtures must be a list of labels")
    elif len(set(planned)) != len(planned) or set(planned) - set(labels):
        problems.append("run has duplicate or unknown planned fixtures")
    elif planned != labels:
        missing.append("the run declares a diagnostic fixture subset/order")
    override = run.get("attempts_override")
    if override is not None:
        if type(override) is not int or override <= 0:
            problems.append("invalid attempts_override")
        else:
            missing.append("an attempts override makes this a diagnostic run")
    if run.get("diagnostic_selection") is True:
        missing.append("an explicit fixture selection makes this a diagnostic run")
    elif "diagnostic_selection" in run and type(run["diagnostic_selection"]) is not bool:
        problems.append("diagnostic_selection must be Boolean")
    if run.get("status") != "completed":
        missing.append(f"run is not completed: {run.get('status')!r}; {run.get('incomplete_reason')}")
    elif run.get("incomplete_reason") is not None:
        problems.append("completed run has an incomplete_reason")
    return missing, problems
