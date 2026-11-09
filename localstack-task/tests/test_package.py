from __future__ import annotations

import os
import subprocess
import zipfile
from pathlib import Path

import pytest

from bikes import package
from bikes.package import (
    INSTALL_SCRIPT,
    LAMBDA_IMAGE,
    PackageError,
    build_package,
    dependencies,
    lambda_architecture,
    pip_command,
)


@pytest.mark.parametrize(
    ("machine", "architecture"),
    [("x86_64", "x86_64"), ("AMD64", "x86_64"), ("aarch64", "arm64"), ("arm64", "arm64")],
)
def test_the_function_runs_on_the_hosts_architecture(machine, architecture):
    assert lambda_architecture(machine) == architecture


def test_pip_runs_in_the_lambda_image_as_the_caller(tmp_path):
    command = pip_command(tmp_path / "req.txt", tmp_path / "out", "arm64", user="1000:1000")

    assert command[:5] == ["docker", "run", "--rm", "--platform", "linux/arm64"]
    assert command[command.index("--user") + 1] == "1000:1000"
    assert f"{(tmp_path / 'out').resolve()}:/out" in command
    assert command[-5:] == ["--entrypoint", "sh", LAMBDA_IMAGE, "-c", INSTALL_SCRIPT]
    assert "linux/amd64" in pip_command(tmp_path / "r", tmp_path / "o", "x86_64")


def test_what_the_runtime_has_is_installed_in_the_runtimes_version():
    listing, install = INSTALL_SCRIPT.split(" && ")

    assert listing.endswith("list --format=freeze > /tmp/runtime.txt")
    assert "--constraint /tmp/runtime.txt" in install
    assert install.endswith("--target /out --requirement /requirements.txt")


class FakePip:
    """Stands in for docker run: installs a fake pandas into the mounted folder"""

    def __init__(self, fail: BaseException | None = None) -> None:
        self.runs = 0
        self.fail = fail

    def __call__(self, command: list[str], check: bool) -> None:
        self.runs += 1
        if self.fail is not None:
            raise self.fail
        mount = next(arg for arg in command if arg.endswith(":/out"))
        target = Path(mount.removesuffix(":/out"))
        files = {
            "pandas/__init__.py": "",
            "pandas/core/frame.py": "",
            "pandas/tests/test_frame.py": "",
            "pandas/__pycache__/__init__.cpython-312.pyc": "",
            "pandas-3.0.5.dist-info/METADATA": "",
            "numpy.libs/libscipy_openblas.so": "",
            "bin/f2py": "",
        }
        for name, content in files.items():
            (target / name).parent.mkdir(parents=True, exist_ok=True)
            (target / name).write_text(content)
        os.chmod(target / "numpy.libs/libscipy_openblas.so", 0o755)


@pytest.fixture
def project(tmp_path):
    requirements = tmp_path / "requirements-lambda.txt"
    requirements.write_text("pandas==3.0.5\nnumpy==2.5.3\n")
    code = tmp_path / "src" / "bikes"
    for name in ("__init__.py", "loader/__init__.py", "loader/handler.py"):
        (code / name).parent.mkdir(parents=True, exist_ok=True)
        (code / name).write_text(f"# {name}\n")
    (code / "__pycache__").mkdir()
    (code / "__pycache__" / "aws.cpython-312.pyc").write_bytes(b"\0")
    return requirements, tmp_path / "build", code


def test_dependencies_are_installed_once_per_requirements_and_architecture(project):
    requirements, build, _ = project
    pip = FakePip()

    first = dependencies("x86_64", requirements, build, run=pip, report=lambda line: None)
    again = dependencies("x86_64", requirements, build, run=pip, report=lambda line: None)
    assert (first, pip.runs) == (again, 1)
    assert first.name.startswith("lambda-deps-x86_64-")

    requirements.write_text("pandas==3.0.6\n")
    dependencies("x86_64", requirements, build, run=pip, report=lambda line: None)
    dependencies("arm64", requirements, build, run=pip, report=lambda line: None)
    assert pip.runs == 3


def test_a_new_install_script_installs_again(project, monkeypatch):
    requirements, build, _ = project
    pip = FakePip()
    dependencies("x86_64", requirements, build, run=pip, report=lambda line: None)

    monkeypatch.setattr(package, "INSTALL_SCRIPT", INSTALL_SCRIPT + " --upgrade")
    dependencies("x86_64", requirements, build, run=pip, report=lambda line: None)

    assert pip.runs == 2


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (subprocess.CalledProcessError(1, "docker"), "pip failed"),
        (FileNotFoundError("docker"), "docker not found"),
    ],
)
def test_a_failed_install_is_explained_and_not_kept(project, failure, message):
    requirements, build, _ = project

    with pytest.raises(PackageError, match=message):
        dependencies("x86_64", requirements, build, run=FakePip(fail=failure))

    pip = FakePip()
    dependencies("x86_64", requirements, build, run=pip, report=lambda line: None)
    assert pip.runs == 1  # the failed attempt left nothing that passes for an install


def test_the_requirements_file_must_be_there(tmp_path):
    with pytest.raises(PackageError, match="run from the project folder"):
        dependencies("x86_64", tmp_path / "missing.txt", tmp_path / "build", run=FakePip())


def build(project, pip=None):
    requirements, build_dir, code = project
    return build_package(
        "x86_64", requirements, build_dir, code, run=pip or FakePip(), report=lambda line: None
    )


def test_the_zip_holds_the_code_and_dependencies_but_no_tests_caches_or_scripts(project):
    package = build(project)

    with zipfile.ZipFile(package.path) as archive:
        names = archive.namelist()
        modes = {info.filename: info.external_attr >> 16 & 0o777 for info in archive.infolist()}
    assert names == sorted(names)
    assert set(names) == {
        "bikes/__init__.py",
        "bikes/loader/__init__.py",
        "bikes/loader/handler.py",
        "numpy.libs/libscipy_openblas.so",
        "pandas-3.0.5.dist-info/METADATA",
        "pandas/__init__.py",
        "pandas/core/frame.py",
    }
    assert modes["numpy.libs/libscipy_openblas.so"] == 0o755
    assert modes["bikes/loader/handler.py"] == 0o644
    assert package.files == len(names)


def test_the_same_files_make_the_same_zip(project):
    _, _, code = project
    first = build(project)
    os.utime(code / "loader" / "handler.py", (1_000_000_000, 1_000_000_000))  # touched only

    assert build(project).sha256 == first.sha256

    (code / "loader" / "handler.py").write_text("# changed\n")
    assert build(project).sha256 != first.sha256


def pins(path: Path) -> dict[str, str]:
    lines = (line.strip() for line in path.read_text().splitlines())
    return {line.split("==")[0]: line for line in lines if "==" in line and line[0] != "#"}


def test_the_lambda_gets_the_same_pandas_the_tests_run_on():
    root = Path(__file__).resolve().parents[1]
    lambda_pins = pins(root / "requirements-lambda.txt")
    dev_pins = pins(root / "requirements-dev.txt")

    assert lambda_pins
    assert {name: dev_pins.get(name) for name in lambda_pins} == lambda_pins
