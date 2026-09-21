"""Native readings and deterministic failure/projection checks; no experiment."""
import pytest

from tools.pattern_lab import PatternLabDataError
from tools.pattern_lab.analysis import calibration_memory as memory
from tools.pattern_lab.analysis import calibration_monthly as monthly


def test_native_memory_readings():
    process = memory.process_memory()
    host = memory.host_memory()
    assert process["peak_rss_bytes"] > 0
    assert process["current_rss_bytes"] > 0
    assert host["total_bytes"] >= host["available_bytes"] > 0
    if memory.sys.platform == "win32":
        assert host["swap_total_bytes"] is None
        assert "not applicable" in host["unavailable"]["swap_total_bytes"]


@pytest.mark.parametrize("platform", ["win32", "linux"])
@pytest.mark.parametrize("failed", ["peak", "available"])
def test_required_measurements_fail_closed(platform, failed, monkeypatch, tmp_path):
    def unavailable(*args):
        raise OSError("injected measurement failure")

    monkeypatch.setattr(memory.sys, "platform", platform)
    monkeypatch.setattr(memory, "_windows_process", unavailable if failed == "peak" else lambda: (100, 90))
    monkeypatch.setattr(memory, "_unix_peak", unavailable if failed == "peak" else lambda: 100)
    monkeypatch.setattr(memory, "_windows_host", unavailable if failed == "available" else
                        lambda: {"available_bytes": 2**30, "total_bytes": 2**31})
    monkeypatch.setattr(memory, "_proc_values", lambda path: {"VmRSS": 90, "MemTotal": 2**31,
                        **({} if failed == "available" else {"MemAvailable": 2**30})})
    monkeypatch.setattr(monthly, "environment_document", lambda: {
        "host_memory": memory.host_memory(), "process_memory": memory.process_memory()})
    monkeypatch.setattr(monthly, "run_candidate_repetition", lambda *a: pytest.fail("unguarded generation"))
    result = monthly.run_experiment(output_root=tmp_path / "run", attempts=1, include_replays=False)
    assert result["status"] == "incomplete"
    assert result["stops"][0]["kind"] == "measurement_unavailable"
    assert result["budget"]["samples"][0]["unavailable"] or result["budget"]["samples"][0]["host_memory"]["unavailable"]


def test_missing_current_rss_is_only_diagnostic(monkeypatch):
    monkeypatch.setattr(monthly, "process_memory", lambda: {
        "peak_rss_bytes": 100, "current_rss_bytes": None, "unavailable": {"current_rss_bytes": "missing"}})
    monkeypatch.setattr(monthly, "host_memory", lambda: {"available_bytes": 2**30, "unavailable": {}})
    assert monthly.Budget(monthly.time.monotonic()).sample("test")["current_rss_bytes"] is None


def test_failure_reporting_does_not_retry_the_failed_measurement(monkeypatch,tmp_path):
    calls = []
    def failed_process():
        calls.append(1)
        assert len(calls) == 1, "failure reporting retried a failed measurement"
        return {"peak_rss_bytes":None,"current_rss_bytes":None,"unavailable":{"peak_rss_bytes":"denied"}}
    monkeypatch.setattr(monthly,"process_memory",failed_process)
    result = monthly.run_experiment(output_root=tmp_path/'run',attempts=1)
    assert result['status']=='incomplete'
    assert result['budget']['peak_rss_bytes'] is None
    assert calls == [1]


@pytest.mark.parametrize('platform',['linux','win32'])
def test_injected_platform_readings_are_bytes(platform,monkeypatch):
    monkeypatch.setattr(memory.sys,'platform',platform)
    monkeypatch.setattr(memory,'_windows_process',lambda:(8192,4096))
    monkeypatch.setattr(memory,'_unix_peak',lambda:8192)
    monkeypatch.setattr(memory,'_windows_host',lambda:{'total_bytes':32768,'available_bytes':16384})
    monkeypatch.setattr(memory,'_proc_values',lambda path:{'VmRSS':4096,'MemTotal':32768,'MemAvailable':16384,'SwapTotal':0,'SwapFree':0})
    assert memory.process_memory()['peak_rss_bytes']==8192
    assert memory.process_memory()['current_rss_bytes']==4096
    assert memory.host_memory()['available_bytes']==16384


@pytest.mark.parametrize("attempts", [0, -1, True, 1.5, "2"])
def test_attempt_override_requires_positive_integer(attempts, tmp_path):
    with pytest.raises(PatternLabDataError, match="positive integer"):
        monthly.run_experiment(output_root=tmp_path / "unused", attempts=attempts)
    assert not (tmp_path / "unused").exists()


@pytest.mark.parametrize("attempts", [1, 3, 7, None])
def test_requested_projection_and_pilot_reuse(attempts, monkeypatch):
    clock = [0.0]
    calls = []
    def repetition(fixture, identity):
        calls.append(identity)
        clock[0] += 10.0
        return {"id": identity, "available": True, "informative_months": 12,
                "reject_raw": False, "noncoverage": False, "family_any_rejection": False}
    monkeypatch.setattr(monthly.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(monthly, "run_candidate_repetition", repetition)
    monkeypatch.setattr(monthly.Budget, "sample", lambda *a, **kw: {"peak_rss_bytes": 100})
    fixture = monthly.MAIN_MATRIX[0]
    supplement = monthly.SUPPLEMENTARY_MATRIX[0]
    budget = monthly.Budget(0)
    pilot, reuse = monthly._run_pilot([fixture], planned=[fixture, supplement],
                                     budget=budget, attempts=attempts)
    count = fixture.attempts if attempts is None else attempts
    pilot_count = min(5, count)
    assert pilot["attempts_per_fixture"] == pilot_count
    assert pilot["projected_total_seconds"] == 10 * (count - pilot_count) + 10 * (
        supplement.attempts if attempts is None else attempts) + monthly.REPLAY_ALLOWANCE_SECONDS
    # Complete only the small diagnostic cases; the full default projection is draw-free beyond five.
    if attempts is not None:
        monthly.run_fixture(fixture, attempts=count, budget=budget, reuse=reuse[fixture.label])
        assert calls == list(range(20000, 20000 + count))
