from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]


def test_project_pins_torch_to_cuda_126_for_windows_and_linux() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "torch==2.9.1" in pyproject["project"]["dependencies"]

    torch_sources = pyproject["tool"]["uv"]["sources"]["torch"]
    assert {
        "index": "pytorch-cu126",
        "marker": "sys_platform == 'linux' or sys_platform == 'win32'",
    } in torch_sources

    pytorch_indexes = {
        index["name"]: index
        for index in pyproject["tool"]["uv"]["index"]
    }
    assert pytorch_indexes["pytorch-cu126"] == {
        "name": "pytorch-cu126",
        "url": "https://download.pytorch.org/whl/cu126",
        "explicit": True,
    }


def test_lockfile_resolves_torch_from_cuda_126_index() -> None:
    lockfile = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    packages = {package["name"]: package for package in lockfile["package"]}

    torch_package = packages["torch"]
    assert torch_package["version"] == "2.9.1+cu126"
    assert torch_package["source"] == {
        "registry": "https://download.pytorch.org/whl/cu126"
    }
