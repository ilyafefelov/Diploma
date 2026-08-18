from pathlib import Path


WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "public-bess-index.yml"


def test_public_bess_publisher_fails_closed_and_retries_same_day() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert '- cron: "35 5 * * *"' in workflow
    assert '- cron: "35 8 * * *"' in workflow
    assert 'default: true' in workflow
    assert 'DEFAULT_FAIL_ON_FETCH_ERROR: "true"' in workflow
    assert 'args+=(--fail-on-fetch-error)' in workflow
    isolated_runs = workflow.split("uv run --no-project")[1:]
    assert len(isolated_runs) == 2
    assert all("--with 'pydantic>=2,<3'" in run for run in isolated_runs)
    verification_step = workflow.split('name: Verify GitHub raw public JSON freshness', maxsplit=1)[1].split(
        'name: Best-effort trigger Vercel production deploy',
        maxsplit=1,
    )[0]
    assert 'if:' not in verification_step
