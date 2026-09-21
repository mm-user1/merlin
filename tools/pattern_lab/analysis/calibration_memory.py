"""Optional native memory readings for the research driver's sampled guard.

All sizes are bytes. Unavailable readings carry reasons, never invented zeros.
Importing this module does not load a platform API or take a measurement.

Native field definitions:
https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters
https://learn.microsoft.com/en-us/windows/win32/api/sysinfoapi/ns-sysinfoapi-memorystatusex
"""

from pathlib import Path
import sys


def _windows_process():
    import ctypes
    from ctypes import wintypes

    class Counters(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in (
                "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage",
            )
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        raise ctypes.WinError(ctypes.get_last_error())
    return int(counters.PeakWorkingSetSize), int(counters.WorkingSetSize)


def _windows_host():
    import ctypes
    from ctypes import wintypes

    class Status(ctypes.Structure):
        _fields_ = [("dwLength", wintypes.DWORD), ("dwMemoryLoad", wintypes.DWORD)] + [
            (name, ctypes.c_ulonglong) for name in (
                "ullTotalPhys", "ullAvailPhys", "ullTotalPageFile", "ullAvailPageFile",
                "ullTotalVirtual", "ullAvailVirtual", "ullAvailExtendedVirtual",
            )
        ]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GlobalMemoryStatusEx.argtypes = [ctypes.POINTER(Status)]
    kernel.GlobalMemoryStatusEx.restype = wintypes.BOOL
    status = Status()
    status.dwLength = ctypes.sizeof(status)
    if not kernel.GlobalMemoryStatusEx(ctypes.byref(status)):
        raise ctypes.WinError(ctypes.get_last_error())
    # PageFile fields describe commit limits, not Linux swap; do not relabel them.
    return {"total_bytes": int(status.ullTotalPhys), "available_bytes": int(status.ullAvailPhys)}


def _unix_peak():
    import resource

    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(usage) * (1 if sys.platform == "darwin" else 1024)


def _proc_values(path):
    values = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        key, _, rest = line.partition(":")
        parts = rest.split()
        if len(parts) == 2 and parts[1] == "kB":
            values[key] = int(parts[0]) * 1024
    return values


def process_memory():
    readings = {"peak_rss_bytes": None, "current_rss_bytes": None, "unavailable": {}}
    if sys.platform == "win32":
        try:
            readings["peak_rss_bytes"], readings["current_rss_bytes"] = _windows_process()
        except (OSError, ValueError, AttributeError) as error:
            for key in ("peak_rss_bytes", "current_rss_bytes"):
                readings["unavailable"][key] = str(error)
    else:
        try:
            readings["peak_rss_bytes"] = _unix_peak()
        except (ImportError, OSError, ValueError, AttributeError) as error:
            readings["unavailable"]["peak_rss_bytes"] = str(error)
        try:
            readings["current_rss_bytes"] = _proc_values("/proc/self/status").get("VmRSS")
        except (OSError, ValueError) as error:
            readings["unavailable"]["current_rss_bytes"] = str(error)
    for key in ("peak_rss_bytes", "current_rss_bytes"):
        if type(readings[key]) is not int or readings[key] <= 0:
            readings[key] = None
            readings["unavailable"].setdefault(key, "resident-memory reading missing or invalid")
    return readings


def host_memory():
    readings = dict.fromkeys(("total_bytes", "available_bytes", "swap_total_bytes", "swap_free_bytes"))
    reasons = {}
    try:
        if sys.platform == "win32":
            readings.update(_windows_host())
            reasons.update({key: "Linux swap diagnostic is not applicable on Windows"
                            for key in ("swap_total_bytes", "swap_free_bytes")})
        else:
            values = _proc_values("/proc/meminfo")
            readings.update({key: values.get(field) for key, field in (
                ("total_bytes", "MemTotal"), ("available_bytes", "MemAvailable"),
                ("swap_total_bytes", "SwapTotal"), ("swap_free_bytes", "SwapFree"),
            )})
    except (OSError, ValueError, AttributeError) as error:
        reasons.update({key: str(error) for key in readings})
    for key, value in readings.items():
        if type(value) is not int or value < 0:
            readings[key] = None
            reasons.setdefault(key, "memory reading missing or invalid")
    readings["unavailable"] = reasons
    return readings
