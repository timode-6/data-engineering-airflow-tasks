"""The loader Lambda's deployment package: the bikes code, pandas and numpy, in one zip"""

from __future__ import annotations

import base64
import hashlib
import os
import platform
import shutil
import stat
import subprocess
import zipfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

import bikes

LAMBDA_IMAGE = "public.ecr.aws/lambda/python:3.12"
PIP = "python3 -m pip --disable-pip-version-check"
INSTALL_SCRIPT = (
    f"{PIP} list --format=freeze > /tmp/runtime.txt && "
    f"{PIP} install --no-cache-dir --no-compile --constraint /tmp/runtime.txt "
    "--target /out --requirement /requirements.txt"
)
REQUIREMENTS = Path("requirements-lambda.txt")
BUILD = Path("build")
PACKAGE_NAME = "bike-loader.zip"
LEFT_OUT = frozenset({"__pycache__", "tests"})
ZIP_TIME = (1980, 1, 1, 0, 0, 0)

Run = Callable[..., object]
Report = Callable[[str], None]


class PackageError(RuntimeError):
    """The package could not be built; the message says why"""


@dataclass(frozen=True)
class Package:
    path: Path
    architecture: str
    sha256: str
    files: int

    @property
    def size(self) -> int:
        return self.path.stat().st_size


def lambda_architecture(machine: str | None = None) -> str:
    """The host's architecture, so Docker runs the function without emulation"""
    machine = (machine or platform.machine()).lower()
    return "arm64" if machine in ("arm64", "aarch64") else "x86_64"


def docker_user() -> str | None:
    """uid:gid to run pip as, so its files belong to the caller; None where there is none"""
    if not hasattr(os, "getuid"):
        return None
    return f"{os.getuid()}:{os.getgid()}"


def pip_command(
    requirements: Path, target: Path, architecture: str, user: str | None = None
) -> list[str]:
    """docker run, in the Lambda image: INSTALL_SCRIPT, with target as its /out"""
    docker_platform = "linux/arm64" if architecture == "arm64" else "linux/amd64"
    command = ["docker", "run", "--rm", "--platform", docker_platform]
    if user is not None:
        command += ["--user", user, "--env", "HOME=/tmp"]
    command += ["--volume", f"{requirements.resolve()}:/requirements.txt:ro"]
    command += ["--volume", f"{target.resolve()}:/out"]
    return [*command, "--entrypoint", "sh", LAMBDA_IMAGE, "-c", INSTALL_SCRIPT]


def dependencies(
    architecture: str,
    requirements: Path = REQUIREMENTS,
    build: Path = BUILD,
    run: Run = subprocess.run,
    report: Report = print,
) -> Path:
    """The folder holding what requirements lists, installed for architecture"""
    if not requirements.is_file():
        raise PackageError(f"{requirements} not found; run from the project folder")
    recipe = "\n".join([LAMBDA_IMAGE, INSTALL_SCRIPT, requirements.read_text()])
    digest = hashlib.sha256(recipe.encode()).hexdigest()[:12]
    target = build / f"lambda-deps-{architecture}-{digest}"
    if target.is_dir():
        report(f"Dependencies: {target}, installed by an earlier deploy")
        return target
    partial = target.with_name(f"{target.name}.partial")
    shutil.rmtree(partial, ignore_errors=True)
    partial.mkdir(parents=True)  # made here, not by Docker, which would make it root's
    report(f"Dependencies: pip installs {requirements} in {LAMBDA_IMAGE} ({architecture})")
    try:
        run(pip_command(requirements, partial, architecture, docker_user()), check=True)
    except FileNotFoundError as error:
        raise PackageError("docker not found: the package is built in a container") from error
    except subprocess.CalledProcessError as error:
        raise PackageError("pip failed in the Lambda image; its output is above") from error
    partial.rename(target)
    return target


def is_packaged(relative: Path) -> bool:
    """Whether a file, given relative to its folder, goes into the zip"""
    return relative.parts[0] != "bin" and not LEFT_OUT.intersection(relative.parts)


def package_files(dependencies: Path, code: Path) -> list[tuple[str, Path]]:
    """(name in the zip, file on disk) for every file the function needs, sorted"""
    found = [
        (path.relative_to(dependencies).as_posix(), path)
        for path in dependencies.rglob("*")
        if path.is_file() and is_packaged(path.relative_to(dependencies))
    ]
    found += [
        (f"bikes/{path.relative_to(code).as_posix()}", path)
        for path in code.rglob("*.py")
        if is_packaged(path.relative_to(code))
    ]
    return sorted(found)


def write_zip(files: Sequence[tuple[str, Path]], path: Path) -> None:
    """Zip files with a fixed time and mode each, so equal files give an equal zip"""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.partial")
    with zipfile.ZipFile(partial, "w") as archive:
        for name, source in files:
            info = zipfile.ZipInfo(name, date_time=ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            mode = 0o755 if source.stat().st_mode & 0o111 else 0o644
            info.external_attr = (stat.S_IFREG | mode) << 16
            with source.open("rb") as reader, archive.open(info, "w") as writer:
                shutil.copyfileobj(reader, writer, 2**20)
    partial.replace(path)


def code_sha256(path: Path) -> str:
    """The zip's SHA-256 in base64, comparable with the function's CodeSha256"""
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").digest()
    return base64.b64encode(digest).decode()


def build_package(
    architecture: str,
    requirements: Path = REQUIREMENTS,
    build: Path = BUILD,
    code: Path | None = None,
    run: Run = subprocess.run,
    report: Report = print,
) -> Package:
    """build/bike-loader.zip: the dependencies for architecture, and the bikes code"""
    installed = dependencies(architecture, requirements, build, run, report)
    files = package_files(installed, code or Path(bikes.__file__).parent)
    path = build / PACKAGE_NAME
    write_zip(files, path)
    package = Package(path, architecture, code_sha256(path), files=len(files))
    report(f"Package: {path}, {package.files:,} files, {package.size / 2**20:.1f} MiB")
    return package
