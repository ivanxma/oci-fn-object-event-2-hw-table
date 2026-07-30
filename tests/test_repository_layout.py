from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_production_directories_do_not_contain_test_harnesses() -> None:
    forbidden = {
        ROOT / "processor" / "test_stream_processor.py",
        ROOT / "processor" / "verify_capture_store.py",
        ROOT / "deploy" / "verify_parallel_flow.py",
        ROOT / "deploy" / "verify_parallel_flow.sh",
        ROOT / "deploy" / "verify_durable_capture.sh",
        ROOT / "deploy" / "verify_streaming_deployment.sh",
    }
    assert not [path for path in forbidden if path.exists()]


def test_disposable_sql_fixtures_are_kept_under_tests() -> None:
    fixture_directory = ROOT / "tests" / "fixtures" / "sql"
    fixtures = sorted(fixture_directory.glob("*.sql"))
    assert {path.name for path in fixtures} == {
        "create_employees_parallel_verification.sql",
        "create_employees_verification.sql",
    }
    for fixture in fixtures:
        source = fixture.read_text(encoding="utf-8")
        assert "EMPLOYEE_ID BIGINT NOT NULL" in source
        assert "batch_num BIGINT UNSIGNED NOT NULL INVISIBLE" in source
