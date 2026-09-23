from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_service_status_reports_ui_processor_release_and_logs():
    source = (ROOT / "deploy" / "service_status.sh").read_text()
    assert 'UI_SERVICE_NAME="${UI_SERVICE_NAME:-object-storage-heatwave-ui}"' in source
    assert 'container-instances container-instance list' in source
    assert 'container-instances container-instance get' in source
    assert '"release-version"' in source
    assert 'journalctl --no-pager -u "$UI_SERVICE_NAME"' in source
    assert 'Processor logs: not exported to this VM.' in source


def test_service_status_does_not_print_sensitive_runtime_values():
    source = (ROOT / "deploy" / "service_status.sh").read_text()
    assert 'RELEASE_VERSION|GIT_SHA|SOURCE_BRANCH|BUILD_UTC|UI_IMAGE_TAG' in source
    assert 'FLASK_SECRET_KEY' not in source
    assert 'DB_SECRET_OCID' not in source
