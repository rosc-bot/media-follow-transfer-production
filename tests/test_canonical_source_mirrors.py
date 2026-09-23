from pathlib import Path


def _application_sources(root: Path) -> dict[str, bytes]:
    return {
        file.relative_to(root).as_posix(): file.read_bytes()
        for file in sorted(root.rglob("*"))
        if file.is_file() and "__pycache__" not in file.parts and file.suffix != ".pyc"
    }


def test_api_follow_and_transfer_runtime_apps_mirror_canonical_app_tree():
    repository = Path(__file__).resolve().parents[1]
    canonical = _application_sources(repository / "app")

    assert canonical
    for service in ("api", "follow-worker", "transfer-worker"):
        runtime = _application_sources(repository / "production-runtime" / "services" / service / "app")
        assert runtime == canonical, f"{service} source drifted from the canonical application tree"
