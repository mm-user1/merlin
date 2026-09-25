"""Trusted Python extensions, source digests and environment provenance.

One source-integrity mechanism is used: digest verification.  Declared custom
module and helper files are hashed before import and registration, verified
again before each job and before a job's result is accepted, and any detected
change fails the run.  Snapshots copied into a run are inert provenance and are
never imported or executed.

This checks cooperating stable source files.  It is not a sandbox: trusted
Python can still open a path of its own, and arbitrary causality cannot be
proved by shape checks.
"""

from __future__ import annotations

from dataclasses import dataclass
import builtins
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import platform
import re
import subprocess
import sys
import sysconfig
from types import ModuleType
from typing import Any, Sequence

from .. import PatternLabDataError

# The core modules a study actually consumes.  This is an explicit list, not a
# crawler over the repository or the environment.
CORE_MODULES = (
    "tools.pattern_lab.data",
    "tools.pattern_lab.candidate",
    "tools.pattern_lab.manifest",
    "tools.pattern_lab.pack_lock",
    "tools.pattern_lab.update_transaction",
    "tools.pattern_lab.study.contracts",
    "tools.pattern_lab.study.context",
    "tools.pattern_lab.study.builtins",
    "tools.pattern_lab.study.spec",
    "tools.pattern_lab.study.validation",
    "tools.pattern_lab.study.extensions",
    "tools.pattern_lab.study.observations",
    "tools.pattern_lab.study.job",
    "tools.pattern_lab.study.workers",
    "tools.pattern_lab.study.evidence",
    "tools.pattern_lab.study.runner",
    "tools.pattern_lab.study.results",
    "tools.pattern_lab.study.report",
)


def file_digest(path: Path) -> str:
    """Return the SHA-256 of one source file's exact bytes."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class LoadedExtension:
    """One imported trusted module plus the digests of its declared sources."""

    module: str
    source_root: str
    module_path: str
    files: tuple[tuple[str, str], ...]

    def as_json(self) -> dict[str, Any]:
        return {
            "module": self.module,
            "source_root": self.source_root,
            "module_path": self.module_path,
            "files": [{"path": path, "sha256": digest} for path, digest in self.files],
        }


# Modules this interpreter has already imported as verified extensions, with the
# digests they were imported from.  A second study reuses them only when the
# files still hash to the same values.
_LOADED: dict[str, LoadedExtension] = {}
# Runtime object, resolved source path and verified executed bytes. Records are
# committed only after the owning extension successfully registers.
_VERIFIED_MODULES: dict[str, tuple[ModuleType, Path, str]] = {}
_VERIFIED_IMPORTS: dict[str, tuple[ModuleType, ...]] = {}
_IMPORT_MODULE = importlib.import_module
_ACTIVE_IMPORT_WINDOW = None


def _dispatch_import_module(name, package=None):
    """Retained aliases consult the current window, never an expired closure."""
    window = _ACTIVE_IMPORT_WINDOW
    if window is None:
        delegate = importlib.import_module
        return (delegate if delegate is not _dispatch_import_module else _IMPORT_MODULE)(name, package)
    delegate, observe = window
    owner = sys._getframe(1).f_globals.get("__name__")
    imported = delegate(name, package)
    full = importlib.util.resolve_name(name, package) if name.startswith(".") else name
    observe(full, owner)
    return imported


def _relative_source_path(value, where):
    if not isinstance(value, str) or not value:
        raise PatternLabDataError(f"{where}: expected a relative source path.")
    path = PurePosixPath(value.replace("\\", "/"))
    if path.is_absolute() or PureWindowsPath(value).drive or ".." in path.parts or not path.parts:
        raise PatternLabDataError(f"{where}: invalid relative source path {value!r}.")
    return path.as_posix()


def source_records(records, declarations, *, where):
    """Validate and project saved/loaded attribution without physical roots.

    Check uniqueness before constructing maps, including declared coverage.
    This is also the offline frozen-candidate comparison boundary.
    """
    from . import contracts
    expected = {}
    if not isinstance(declarations, (list, tuple)) or not isinstance(records, (list, tuple)):
        raise PatternLabDataError(f"{where}: extensions and declarations must be lists.")
    for declaration in declarations:
        declaration = contracts.require_mapping(declaration, where)
        name = contracts.require_identifier(declaration.get("module"), where + ".module")
        helpers = declaration.get("helpers")
        if name in expected or not isinstance(helpers, list):
            raise PatternLabDataError(f"{where}: duplicate module {name!r} or invalid helpers.")
        paths = [name + ".py", *helpers]
        checked = set()
        for path in paths:
            path = _relative_source_path(path, where)
            if path in checked:
                raise PatternLabDataError(f"{where}: duplicate source path {name}/{path}.")
            checked.add(path)
        expected[name] = checked
    result = {}
    for record in records:
        record = contracts.require_mapping(record, where)
        name = contracts.require_identifier(record.get("module"), where + ".module")
        if name in result or name not in expected or not isinstance(record.get("files"), list):
            raise PatternLabDataError(f"{where}: duplicate/unexpected module {name!r} or missing files.")
        files = {}
        for item in record["files"]:
            item = contracts.require_mapping(item, where + ".files")
            path, digest = item.get("path"), item.get("sha256")
            path = _relative_source_path(path, where)
            if path not in expected[name] or path in files:
                raise PatternLabDataError(f"{where}: duplicate/unexpected helper {name}/{path}.")
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise PatternLabDataError(f"{where}: invalid SHA-256 for {name}/{path}.")
            files[path] = digest
        if set(files) != expected[name]:
            raise PatternLabDataError(f"{where}: missing declared source/helper coverage for {name}.")
        result[name] = files
    if set(result) != set(expected):
        raise PatternLabDataError(f"{where}: missing declared extension coverage.")
    return result


def compare_source_records(actual, required, declarations, *, where):
    expected = source_records(required, declarations, where=where + " frozen sources")
    observed = source_records(actual, declarations, where=where + " actual sources")
    for name, files in expected.items():
        for path, digest in files.items():
            if observed[name][path] != digest:
                raise PatternLabDataError(
                    f"{where}: extension source {name}/{path}: expected {digest}, "
                    f"actual {observed[name][path]}; frozen generation mismatch.",
                    error_code="source_changed",
                )


def _load_verified_extension(record):
    """Observe ordinary imports only during trusted loading/registration.

    The temporary finder delegates resolution to PathFinder and changes only
    local source code loading, compiling verified .py bytes without a pyc cache.
    The import observer also sees already-cached scalar/namespace imports.
    Arbitrary later dynamic loading remains the trusted author's responsibility.
    """
    global _ACTIVE_IMPORT_WINDOW
    if _ACTIVE_IMPORT_WINDOW is not None:
        raise PatternLabDataError("Extension loading windows must not overlap.")
    from . import contracts
    root = Path(record.source_root).resolve()
    allowed = {(root / path).resolve(): digest for path, digest in record.files}
    pending = {}
    imported_dependencies = {}
    modules_before = set(sys.modules)
    registry_before = {kind: dict(items) for kind, items in contracts._REGISTRY.items()}
    original_import = builtins.__import__
    original_import_module = importlib.import_module
    original_path = list(sys.path)

    def declared(path):
        if path not in allowed:
            raise PatternLabDataError(
                f"extension {record.module}: undeclared local helper {path.relative_to(root)}; declare it in helpers.")

    def check_module(module, seen=None):
        seen = set() if seen is None else seen
        if id(module) in seen:
            return
        seen.add(id(module))
        origin = getattr(module, "__file__", None)
        if origin is None:
            return
        path = Path(origin).resolve()
        if root not in path.parents:
            local = root.joinpath(*module.__name__.split("."))
            for chosen in (local.with_suffix(".py"), local/"__init__.py"):
                if chosen.is_file():
                    raise PatternLabDataError(
                        f"extension {record.module}: helper {module.__name__} is cached from {path}, "
                        f"not chosen source {chosen}. Start a fresh interpreter.", error_code="unverified_module")
            return
        declared(path)
        known = pending.get(module.__name__) or _VERIFIED_MODULES.get(module.__name__)
        if known != (module, path, allowed[path]):
            raise PatternLabDataError(
                f"extension {record.module}: helper {module.__name__} at {path} has an unknown "
                "or changed runtime source generation. Start a fresh interpreter and run again.",
                error_code="unverified_module")
        for dependency in _VERIFIED_IMPORTS.get(module.__name__, ()):
            check_module(dependency, seen)

    class VerifiedLoader(importlib.machinery.SourceFileLoader):
        def get_code(self, fullname):
            path = Path(self.path).resolve()
            declared(path)
            payload = path.read_bytes()
            actual = hashlib.sha256(payload).hexdigest()
            if actual != allowed[path]:
                raise PatternLabDataError(
                    f"extension {record.module}: {path.name}: expected {allowed[path]}, actual {actual}.",
                    error_code="source_changed")
            pending[fullname] = (sys.modules[fullname], path, actual)
            return compile(payload, str(path), "exec", dont_inherit=True)

    class LocalSourceFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            found = importlib.machinery.PathFinder.find_spec(fullname, path)
            if found is not None and found.origin:
                origin = Path(found.origin).resolve()
                if root in origin.parents:
                    declared(origin)
                    found.loader = VerifiedLoader(fullname, str(origin))
                    return found
            return None

    def observe_modules(full, owner, fromlist=()):
        """One declaration/generation check and dependency record for both routes."""
        names = [".".join(full.split(".")[:i]) for i in range(1, len(full.split(".")) + 1)]
        names.extend(full + "." + item for item in (fromlist or ()) if item != "*")
        for imported_name in names:
            module = sys.modules.get(imported_name)
            if module is not None:
                check_module(module)
                origin = getattr(module, "__file__", None)
                if owner in pending and origin and root in Path(origin).resolve().parents:
                    imported_dependencies.setdefault(owner, {})[imported_name] = module

    def observed_import(name, globals=None, locals=None, fromlist=(), level=0):
        imported = original_import(name, globals, locals, fromlist, level)
        full = (importlib.util.resolve_name("." * level + name, globals.get("__package__"))
                if level else name)
        observe_modules(full, (globals or {}).get("__name__"), fromlist)
        return imported

    # Refresh resolution before installing any owned hook/path state. An
    # invalidator failure must propagate without a partially installed window.
    importlib.invalidate_caches()
    finder = LocalSourceFinder()
    try:
        sys.path.insert(0, str(root))
        sys.meta_path.insert(0, finder)
        builtins.__import__ = observed_import
        delegate = original_import_module if original_import_module is not _dispatch_import_module else _IMPORT_MODULE
        _ACTIVE_IMPORT_WINDOW = (delegate, observe_modules)
        importlib.import_module = _dispatch_import_module
        loader = VerifiedLoader(record.module, record.module_path)
        specification = importlib.util.spec_from_file_location(record.module, record.module_path, loader=loader)
        module = importlib.util.module_from_spec(specification)
        sys.modules[record.module] = module
        loader.exec_module(module)
        register = getattr(module, "register", None)
        if not callable(register):
            raise PatternLabDataError(f"extension {record.module!r}: the module must expose a callable register(context).")
        context = ExtensionContext(record.module, record.module_path, dict(record.files)[record.module + ".py"])
        register(context)
        verify_extensions((record,), where="extension after import and registration")
        contracts.require_verified_registrations({record.module_path: dict(record.files)[record.module + ".py"]})
        for name in pending:
            check_module(sys.modules[name])
    except BaseException:
        for kind, items in contracts._REGISTRY.items():
            for name in set(items) - set(registry_before[kind]):
                del items[name]
        for name in set(sys.modules) - modules_before:
            origin = getattr(sys.modules[name], "__file__", None)
            if name == record.module or (origin and root in Path(origin).resolve().parents):
                sys.modules.pop(name, None)
        raise
    finally:
        _ACTIVE_IMPORT_WINDOW = None
        importlib.import_module = original_import_module
        builtins.__import__ = original_import
        sys.meta_path.remove(finder)
        sys.path[:] = original_path
    _VERIFIED_MODULES.update(pending)
    _VERIFIED_IMPORTS.update({name: tuple(imported_dependencies.get(name, {}).values()) for name in pending})


def _declared_files(declaration) -> list[tuple[str, Path]]:
    root = Path(declaration.source_root)
    if not root.is_dir():
        raise PatternLabDataError(
            f"extension {declaration.module!r}: source root {root} is not an existing directory."
        )
    module_file = root / f"{declaration.module}.py"
    if not module_file.is_file():
        raise PatternLabDataError(
            f"extension {declaration.module!r}: {module_file} does not exist. Declare the exact "
            "module file; there is no directory-wide discovery."
        )
    files: list[tuple[str, Path]] = [(f"{declaration.module}.py", module_file)]
    for helper in declaration.helpers:
        candidate = root / helper
        if not candidate.is_file():
            raise PatternLabDataError(
                f"extension {declaration.module!r}: declared helper {helper!r} does not exist at {candidate}."
            )
        files.append((helper, candidate))
    return files


def _resolve_digests(declaration) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((name, file_digest(path)) for name, path in _declared_files(declaration)))


def declared_source_records(declarations):
    """Hash every declared main/helper file before importing any extension."""
    return [{"module": declaration.module, "files": [
        {"path": name, "sha256": digest} for name, digest in _resolve_digests(declaration)
    ]} for declaration in declarations]


def load_extensions(declarations: Sequence[Any]) -> tuple[LoadedExtension, ...]:
    """Hash, import and register the declared trusted modules.

    A module already present in this interpreter is reused only when it was
    imported by this mechanism from exactly the same digests.  Anything else is
    an actionable fresh-interpreter error, because the source generation behind
    the existing registration cannot be established.
    """
    from . import contracts

    loaded: list[LoadedExtension] = []
    for declaration in declarations:
        digests = _resolve_digests(declaration)
        root = str(Path(declaration.source_root))
        module_path = str(Path(declaration.source_root) / f"{declaration.module}.py")
        record = LoadedExtension(
            module=declaration.module, source_root=root, module_path=module_path, files=digests
        )
        previous = _LOADED.get(declaration.module)
        if previous is not None:
            runtime = _VERIFIED_MODULES.get(declaration.module)
            if previous.files == digests and previous.source_root != root:
                raise PatternLabDataError(
                    f"extension {declaration.module!r}: its source root changed after import. "
                    "Relocation requires a fresh interpreter even when source bytes match.",
                    error_code="source_changed",
                )
            if (previous.files != digests or previous.source_root != root
                    or runtime is None or sys.modules.get(declaration.module) is not runtime[0]):
                raise PatternLabDataError(
                    f"extension {declaration.module!r}: its declared source changed after it was "
                    "imported into this interpreter, so the registrations it created cannot be "
                    "attributed to the current files. Start a fresh interpreter and run again.",
                    error_code="source_changed",
                )
            loaded.append(previous)
            continue
        if declaration.module in sys.modules:
            raise PatternLabDataError(
                f"extension {declaration.module!r} is already imported in this interpreter by some "
                "other path, so its source generation cannot be established. Start a fresh "
                "interpreter and run again.",
                error_code="unverified_module",
            )
        _load_verified_extension(record)
        _LOADED[declaration.module] = record
        loaded.append(record)

    contracts.require_verified_registrations(
        {item.module_path: dict(item.files)[f"{item.module}.py"] for item in loaded}
    )
    return tuple(loaded)


@dataclass(frozen=True)
class ExtensionContext:
    """What a trusted module's ``register`` receives.

    Registration is explicit: the module calls the ``register_*`` helpers with
    its own descriptors, so nothing is discovered, monkeypatched or replaced.
    """

    module: str
    source_path: str
    source_digest: str

    def register_feature(self, descriptor) -> None:
        from . import contracts

        contracts.register_feature(
            descriptor, source_digest=self.source_digest, source_path=self.source_path
        )

    def register_hypothesis(self, descriptor) -> None:
        from . import contracts

        contracts.register_hypothesis(
            descriptor, source_digest=self.source_digest, source_path=self.source_path
        )

    def register_model(self, descriptor) -> None:
        from . import contracts

        contracts.register_model(
            descriptor, source_digest=self.source_digest, source_path=self.source_path
        )

    def register_metric(self, descriptor) -> None:
        from . import contracts

        contracts.register_metric(
            descriptor, source_digest=self.source_digest, source_path=self.source_path
        )


def require_frozen_generation(
    declarations: Sequence[Any], frozen: Sequence[LoadedExtension], *, where: str
) -> None:
    """Check every declared file against another process's frozen generation.

    ``frozen`` holds the coordinator's own records for the same declarations.
    A worker uses this before it imports or registers anything, and again after
    import, so a module *or helper* edited between the coordinator's freeze and
    the child's import is rejected instead of being hashed into a fresh local
    generation.  Declaration and record coverage must agree exactly: a file the
    coordinator never froze is as unusable as one that changed.
    """
    expected = {record.module: dict(record.files) for record in frozen}
    for declaration in declarations:
        wanted = expected.get(declaration.module)
        if wanted is None:
            raise PatternLabDataError(
                f"{where}: extension {declaration.module!r} has no frozen source record from the "
                "coordinator, so its source generation cannot be established here.",
                error_code="unverified_source",
            )
        declared = {f"{declaration.module}.py", *declaration.helpers}
        if declared != set(wanted):
            raise PatternLabDataError(
                f"{where}: extension {declaration.module!r} declares {sorted(declared)} but the "
                f"coordinator froze {sorted(wanted)}; the declaration and the frozen record must "
                "cover exactly the same files.",
                error_code="unverified_source",
            )
        root = Path(declaration.source_root)
        for name in sorted(declared):
            path = root / name
            if not path.is_file():
                raise PatternLabDataError(
                    f"{where}: declared extension source {path} is missing.",
                    error_code="source_changed",
                )
            actual = file_digest(path)
            if actual != wanted[name]:
                raise PatternLabDataError(
                    f"{where}: declared extension source {path} hashes to {actual} here, but the "
                    f"coordinator froze {wanted[name]}; the run is stopped rather than mixing "
                    "source generations.",
                    error_code="source_changed",
                )


def verify_extensions(loaded: Sequence[LoadedExtension], *, where: str) -> None:
    """Re-hash every declared source file and fail on any detected change."""
    for record in loaded:
        root = Path(record.source_root)
        for name, expected in record.files:
            path = root / name
            if not path.is_file():
                raise PatternLabDataError(
                    f"{where}: declared extension source {path} is missing.", error_code="source_changed"
                )
            actual = file_digest(path)
            if actual != expected:
                raise PatternLabDataError(
                    f"{where}: declared extension source {path} changed (expected {expected}, "
                    f"found {actual}); the run is stopped rather than mixing source generations.",
                    error_code="source_changed",
                )


# --------------------------------------------------------------------------
# implementation identity and environment provenance
# --------------------------------------------------------------------------

def core_source_digests() -> dict[str, str]:
    """Digest the consumed core modules that are importable as files."""
    digests: dict[str, str] = {}
    for name in CORE_MODULES:
        module = sys.modules.get(name)
        origin = getattr(module, "__file__", None) if module is not None else None
        if origin is None:
            specification = importlib.util.find_spec(name)
            origin = None if specification is None else specification.origin
        if origin and Path(origin).is_file():
            digests[name] = file_digest(Path(origin))
    return digests


def bracket_source_digests():
    """Bounded consumed-contract attribution without importing generic core."""
    root = Path(__file__).resolve().parents[3]
    paths = ["src/core/engine_v2/"+name+".py" for name in
             ("kernel", "sizing", "contracts", "price_rounding", "execution_modes", "diagnostics")]
    paths += ["src/core/backtest_engine.py"]
    paths += ["tools/pattern_lab/study/"+name+".py" for name in ("bracket", "bracket_rules", "sequential", "sequential_checks")]
    return {path:file_digest(root/path) for path in paths}


def library_versions() -> dict[str, str]:
    """Return the numerical and storage library versions actually used."""
    versions = {"python": platform.python_version()}
    for name in ("numpy", "pandas", "pyarrow"):
        module = sys.modules.get(name)
        if module is None:
            try:
                module = importlib.import_module(name)
            except ImportError:
                continue
        versions[name] = getattr(module, "__version__", "unknown")
    return versions


def git_state(repository_root: Path) -> dict[str, Any]:
    """Return the repository commit and dirty state, or an explicit unknown."""
    root = Path(repository_root)
    if not (root / ".git").exists():
        return {"available": False, "commit": None, "dirty": None}
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, timeout=30, check=True
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, timeout=30, check=True
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {"available": False, "commit": None, "dirty": None}
    return {"available": True, "commit": commit, "dirty": bool(status.strip())}


def environment_provenance(repository_root: Path) -> dict[str, Any]:
    """Compact physical/environment provenance, kept out of semantic identity."""
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "os_name": os.name,
        "executable": sys.executable,
        "platform_tag": sysconfig.get_platform(),
        "library_versions": library_versions(),
        "git": git_state(repository_root),
    }
