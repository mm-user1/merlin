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
import hashlib
import importlib.util
import os
from pathlib import Path
import platform
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


def _verify_local_imports(module, declaration) -> None:
    """Local imported Python helpers must be explicitly declared and hashed.

    This checks imported namespaces, not arbitrary future Python behavior.
    Trusted code must still declare files it opens or dynamically loads later.
    """
    root = Path(declaration.source_root).resolve()
    allowed = {(root / name).resolve() for name, _ in _declared_files(declaration)}
    seen = set()
    def visit(current):
        if id(current) in seen:
            return
        seen.add(id(current))
        for value in vars(current).values():
            imported = value if isinstance(value, ModuleType) else sys.modules.get(getattr(value, "__module__", ""))
            origin = getattr(imported, "__file__", None)
            if origin is None:
                continue
            path = Path(origin).resolve()
            if root in path.parents:
                if path not in allowed:
                    raise PatternLabDataError(f"extension {declaration.module}: undeclared local helper {path.name}; declare it in helpers.")
                visit(imported)
    visit(module)


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
            if previous.files != digests or previous.source_root != root:
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
        specification = importlib.util.spec_from_file_location(declaration.module, module_path)
        if specification is None or specification.loader is None:
            raise PatternLabDataError(
                f"extension {declaration.module!r}: {module_path} is not an importable Python module."
            )
        module = importlib.util.module_from_spec(specification)
        sys.modules[declaration.module] = module
        inserted = root not in sys.path
        if inserted:
            sys.path.insert(0, root)
        try:
            specification.loader.exec_module(module)
        except BaseException:
            sys.modules.pop(declaration.module, None)
            raise
        finally:
            if inserted and root in sys.path:
                sys.path.remove(root)
        register = getattr(module, "register", None)
        _verify_local_imports(module, declaration)
        if not callable(register):
            sys.modules.pop(declaration.module, None)
            raise PatternLabDataError(
                f"extension {declaration.module!r}: the module must expose a callable "
                "register(context) that registers its descriptors explicitly."
            )
        context = ExtensionContext(
            module=declaration.module, source_path=module_path, source_digest=dict(digests)[f"{declaration.module}.py"]
        )
        try:
            register(context)
        except BaseException:
            sys.modules.pop(declaration.module, None)
            raise
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
