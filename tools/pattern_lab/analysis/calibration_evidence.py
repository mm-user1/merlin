"""Admission of monthly research evidence, independent of generation and memory APIs.

Format 1 is the delivered archive; format 2 adds plan/source/primary/replay identity.
The decision policy is versioned separately from the unchanged numerical method.
"""
import gzip
import hashlib
import json
import math
import re
import zlib

import numpy as np

from ..study.contracts import semantic_digest
from .estimator import _holm

EVIDENCE_SCHEMA_VERSION = 2
PLAN_VERSION = 1
DECISION_POLICY_VERSION = 2
REPLAY_IDS = [20000, 20001]
SHA256 = re.compile(r"[0-9a-fA-F]{64}")
RESEARCH_ATTRIBUTION_REVISION = 2
SHARED_MONTHLY_MODULE = "tools.pattern_lab.analysis.monthly"
_PLAN1_MAX_MAIN_ATTEMPTS = 16000


# Immutable plan-1 semantic fingerprints captured from accepted d47dfa4 and
# checked against the retained format-1 manifest. Values exclude attribution
# and explanatory prose. This is evidence interpretation, not old generator code.
_PLAN1_FIELDS = (('family', ('record_primary', 'candle_primary_member_id', 'alpha', 'confidence_level')),
 ('seeds', ('master_seed', 'statistical_range', 'pilot_ids')),
 ('decision',
  ('error_envelope', 'min_primary_availability', 'rates', 'impossible_errors', 'impossible_unavailable')),
 ('budget',
  ('wall_clock_seconds',
   'rss_ceiling_bytes',
   'headroom_required_bytes',
   'headroom_reserve_bytes',
   'pilot_attempts')),
 ('validity',
  ('min_informative_months',
   'positive_deletion_denominator',
   'finite_inputs_and_results',
   'nondegenerate_contrast')),
 ('matrix_entry',
  ('order',
   'fixture_id',
   'name',
   'kind',
   'attempts',
   'source',
   'scenario_name',
   'padded_days',
   'required')),
 ('record_contract',
  ('schema_version',
   'master_seed',
   'timeframe_minutes',
   'bars_per_day',
   'instruments',
   'horizons_minutes',
   'directions',
   'primary_member',
   'common_loading',
   'individual_loading',
   'daily_ar_coefficient',
   'daily_level_scale',
   'bar_innovation_scale',
   'student_t_df',
   'signal_p11',
   'signal_p10',
   'signal_stationary_probability',
   'bar_target_probability_state_1',
   'bar_target_probability_state_0',
   'independent_signal_probability',
   'constant_cost_per_observation',
   'confounded_month_shift',
   'confounded_probabilities',
   'thinning_probability',
   'missing_interval_days',
   'short_history_instrument',
   'short_history_start_day')),
 ('candle_contract',
  ('fixture_id',
   'fixture_name',
   'timeframe_minutes',
   'instruments',
   'study_start_utc_day',
   'study_days',
   'warmup_days',
   'common_loading',
   'individual_loading',
   'return_scale',
   'initial_open',
   'high_factor',
   'low_factor',
   'volume_log_scale',
   'condition',
   'occurrence',
   'comparison',
   'commission_pct_per_side',
   'commission_fraction_per_side',
   'horizons_minutes',
   'directions',
   'primary')))
_PLAN1_DIGESTS = (('method', '594a0ac659bd2ffdd09a04021209b59379ca9bd407078c8e0cfd0df4424a083e'),
 ('scope', 'a6395af04b1a37964d27c0b0ecd14aad721ef885a7092762a0a4a3df7442b667'),
 ('family', '273863a18be9b593519e6af881eee37abd988dab01d914e821d8608bcdec3272'),
 ('seeds', 'a5b084aa65e1b6f693a8041a53a862b4c8aca292ea40bb9fb18b192d3b6bc4fb'),
 ('decision', 'c2d884e114b52c92dcd2fbf187c2eada5deea499da0b286c1c08b6be3fbdb638'),
 ('budget', 'fe2d67213b02bc15b74a7a776314cf4cd2077d614be18bd65b540a3fb06dfc8c'),
 ('validity', '2d6370be1c15422fea756f98c86e07d1cc7fdc67c284182f87544a06fd6e8bfd'),
 ('main', '0d8c03de61db4b83f8ea659f06f55b34c857948b221b265483e6607273548ebe'),
 ('supplementary', '35b01b86beaa5e8b17526fa7fa9d54167baa9a0e23a38d33d2eabad713f42acb'),
 ('record_contract', '5185cba0cb0f1bf24f6f9869838e4e484e8d91afa4778a73180baa45e9ecf695'),
 ('candle_contract', '3b84b497077bd286ebb4614b2fa79eba5ec89fd57480a7e4270f36f663d2992d'))
_PLAN1_SCENARIOS = (('null_independent', '5c07b95f2c64ff07d86ce06305d47bb907aeb7608c5755c9d4bb7bd766328849', 0.0),
 ('null_dependent_t5', '0299a12c12dfc6dd16af8e68542e7b03a5d483f4a209502c4ccd0cf5b039154c', 0.0),
 ('null_conditional_confounded', 'bb0ec750ea7453faf04871de69c51274c712e56ee430ba45627f452cd3d6ce25', 0.0),
 ('null_inclusive_parent', '9db7f0d0de2c9a08ddd506f57e46be13304704bd38e25b81d5bb03e8645361ab', 0.0),
 ('null_admission_boundary_336', '26f37488cbfd7be5306c8ce13726082dbaf6042102f90f18ee9504b70fc9f72c', 0.0),
 ('null_dependent_gaussian_companion',
  '8a78eefe52cd1c865ad1a3fcb2eed256636d12b4f6722da5d7222e99a0dfba98',
  0.0),
 ('short_population_84_days', '5cf77bc2a5d3d7516160fbfaa4ac5c88f7b43e420410a932ff31b7532ba56d5f', 0.0),
 ('short_population_180_days', '77d69dc43ef99d2dd01a7e9103d12bdb022ff23c32613c0549b211a430c9bbd3', 0.0),
 ('stress_long_dependence_ar09_p070',
  '0024c23586d3b6067504c85e3c4de328a398915c489111f2bd953cd0dbb40758',
  0.0),
 ('stress_long_dependence_ar09_p097',
  'cfd3bc6273190056f4722de9d0aca6c96eb889225c8a92069ffab3d10ac8d532',
  0.0),
 ('planted_positive_strong', '43e6007f58fb76303239ce5a55c6cdd0e9904cff7033eca234d0e8bff69ebc23', 0.005),
 ('planted_negative_strong', '52d984946955a1059b99ca74c4c0885c7df73955eb233437d618b38b526aeffd', -0.005),
 ('planted_modest', 'e3de59e72296df1f5d95328f60893a59c273505c681416fddd2d980df583f522', 5e-05),
 ('null_confounded_signal_month_v2',
  '3744a1edcf2985e0e2f60df352db85c13d7edea70d91114ddf86ef92be557e10',
  0.0))
_PLAN1_FAMILIES = (('baseline',
  ('baseline__signal|synthetic|tf30m|tf30m.h60m.long',
   'baseline__signal|synthetic|tf30m|tf30m.h60m.short',
   'baseline__signal|synthetic|tf30m|tf30m.h120m.long',
   'baseline__signal|synthetic|tf30m|tf30m.h120m.short',
   'baseline__signal|synthetic|tf30m|tf30m.h240m.long',
   'baseline__signal|synthetic|tf30m|tf30m.h240m.short',
   'baseline__signal|synthetic|tf30m|tf30m.h480m.long',
   'baseline__signal|synthetic|tf30m|tf30m.h480m.short'),
  'baseline__signal|synthetic|tf30m|tf30m.h240m.long'),
 ('parent_child',
  ('baseline__child|synthetic|tf30m|tf30m.h60m.long',
   'baseline__child|synthetic|tf30m|tf30m.h60m.short',
   'baseline__child|synthetic|tf30m|tf30m.h120m.long',
   'baseline__child|synthetic|tf30m|tf30m.h120m.short',
   'baseline__child|synthetic|tf30m|tf30m.h240m.long',
   'baseline__child|synthetic|tf30m|tf30m.h240m.short',
   'baseline__child|synthetic|tf30m|tf30m.h480m.long',
   'baseline__child|synthetic|tf30m|tf30m.h480m.short',
   'baseline__parent|synthetic|tf30m|tf30m.h60m.long',
   'baseline__parent|synthetic|tf30m|tf30m.h60m.short',
   'baseline__parent|synthetic|tf30m|tf30m.h120m.long',
   'baseline__parent|synthetic|tf30m|tf30m.h120m.short',
   'baseline__parent|synthetic|tf30m|tf30m.h240m.long',
   'baseline__parent|synthetic|tf30m|tf30m.h240m.short',
   'baseline__parent|synthetic|tf30m|tf30m.h480m.long',
   'baseline__parent|synthetic|tf30m|tf30m.h480m.short',
   'child_versus_parent|synthetic|tf30m|tf30m.h60m.long',
   'child_versus_parent|synthetic|tf30m|tf30m.h60m.short',
   'child_versus_parent|synthetic|tf30m|tf30m.h120m.long',
   'child_versus_parent|synthetic|tf30m|tf30m.h120m.short',
   'child_versus_parent|synthetic|tf30m|tf30m.h240m.long',
   'child_versus_parent|synthetic|tf30m|tf30m.h240m.short',
   'child_versus_parent|synthetic|tf30m|tf30m.h480m.long',
   'child_versus_parent|synthetic|tf30m|tf30m.h480m.short'),
  'child_versus_parent|synthetic|tf30m|tf30m.h240m.long'))


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


def plan_truth(scenario_name):
    if scenario_name is None:  # causal candle null
        return 0.0
    for name, _, truth in _PLAN1_SCENARIOS:
        if name == scenario_name:
            return truth
    raise ValueError(f"unsupported plan-1 scenario {scenario_name!r}")


def plan_family(scenario_name):
    plan_truth(scenario_name)  # reject unknown names; None explicitly means candle null
    kind = "parent_child" if scenario_name == "null_inclusive_parent" else "baseline"
    _, members, primary = next(item for item in _PLAN1_FAMILIES if item[0] == kind)
    return list(members), primary


def scenario_matches_plan(scenario):
    wanted = dict((name, digest) for name, digest, _ in _PLAN1_SCENARIOS)
    config = {k: v for k, v in scenario.items() if k not in ("note", "caveat")}
    return semantic_digest(config) == wanted.get(scenario.get("name"))


def verifier_problems(*, decision, main, supplementary, max_main_attempts):
    """Check consumed acceptance settings without consulting live generators."""
    expected, fields = dict(_PLAN1_DIGESTS), dict(_PLAN1_FIELDS)
    problems = []
    if semantic_digest(decision) != expected["decision"]:
        problems.append("verifier acceptance settings differ from frozen plan 1")
    for name, entries in (("main", main), ("supplementary", supplementary)):
        if semantic_digest([{k: row[k] for k in fields["matrix_entry"]} for row in entries]) != expected[name]:
            problems.append(f"verifier {name} matrix differs from frozen plan 1")
    if not same(max_main_attempts, _PLAN1_MAX_MAIN_ATTEMPTS):
        problems.append("verifier max_main_attempts differs from frozen plan 1")
    return problems


def manifest_problems(saved, research_modules):
    problems = []
    version = saved.get("schema_version")
    if type(version) is not int or version not in (1, EVIDENCE_SCHEMA_VERSION):
        problems.append("unknown evidence schema_version")
    if not same(saved.get("plan_version", 1 if version == 1 else None), PLAN_VERSION):
        problems.append("unknown or missing plan_version")
    if saved.get("manifest_digest") != semantic_digest({k: v for k, v in saved.items() if k != "manifest_digest"}):
        problems.append("the saved manifest digest does not match its own content")
    expected = dict(_PLAN1_DIGESTS)
    fields = dict(_PLAN1_FIELDS)
    for key in ("method", "scope"):
        if semantic_digest(saved.get(key)) != expected[key]:
            problems.append(f"manifest {key} differs from plan 1")
    # Only semantic fields: neither producer hashes nor mutable explanatory prose.
    for section in ("family", "seeds", "decision", "budget"):
        keys = fields[section]
        actual = saved.get(section, {})
        if not isinstance(actual, dict) or semantic_digest({key: actual.get(key) for key in keys}) != expected[section]:
            problems.append(f"manifest {section} settings differ from plan 1")
    validity = saved.get("formula", {}).get("validity", {})
    if semantic_digest({k: validity.get(k) for k in fields["validity"]}) != expected["validity"]:
        problems.append("manifest mathematical validity settings differ from plan 1")
    matrix = saved.get("matrix", {})
    for group in ("main", "supplementary"):
        entries = matrix.get(group)
        keys = fields["matrix_entry"]
        if not isinstance(entries, list) or semantic_digest(
            [{k: item.get(k) for k in keys} for item in entries]
        ) != expected[group]:
            problems.append(f"manifest {group} matrix differs from frozen plan 1")
    if not same(matrix.get("max_main_attempts"), _PLAN1_MAX_MAIN_ATTEMPTS):
        problems.append("manifest max_main_attempts differs from plan 1")
    generators = saved.get("generators", {})
    for kind in ("record_contract", "candle_contract"):
        actual = generators.get(kind, {})
        keys = fields[kind]
        if semantic_digest({k: actual.get(k) for k in keys}) != expected[kind]:
            problems.append(f"manifest {kind} settings differ from plan 1")
        if kind == "record_contract":
            if actual.get("generator_digest") != semantic_digest({k: v for k, v in actual.items() if k != "generator_digest"}):
                problems.append("record generator digest does not match its own contract")
            for name, _, _ in _PLAN1_SCENARIOS:
                found = [s for s in actual.get("scenarios", []) if s.get("name") == name]
                if len(found) != 1 or not scenario_matches_plan(found[0]):
                    problems.append(f"record generator required fixture {name} missing, duplicated or changed")
    if version == EVIDENCE_SCHEMA_VERSION:
        if not same(saved.get("decision_policy_version"), DECISION_POLICY_VERSION):
            problems.append("new manifest decision_policy_version is missing or unknown")
        attribution = saved.get("research_implementation", {})
        revision = saved.get("research_attribution_revision", 1)
        if type(revision) is not int or revision not in (1, RESEARCH_ATTRIBUTION_REVISION):
            problems.append("unknown research attribution revision")
        required_modules = [name for name in research_modules if revision != 1 or name != SHARED_MONTHLY_MODULE]
        if not isinstance(attribution, dict) or any(
            not isinstance(attribution.get(name), str) or not SHA256.fullmatch(attribution[name])
            for name in required_modules
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
        try:
            return gzip.decompress(packed.read_bytes())
        except (gzip.BadGzipFile, EOFError, zlib.error) as error:
            raise ValueError(f"{label}: invalid compressed record {packed}: {error}") from error
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
