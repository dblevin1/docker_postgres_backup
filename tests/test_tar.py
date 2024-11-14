from docker_postgres_backup.backup import tar_file
from docker_postgres_backup.config import settings
from unittest.mock import patch, MagicMock
from datetime import datetime


@patch("docker_postgres_backup.backup._safe_run")
@patch("docker_postgres_backup.backup.run_rclone")
def test_tar_and_rclone_params(mock_rclone: MagicMock, mock_run_tar: MagicMock):
    settings.BACKUP_LOCATION = "/tmp/test_dir"
    settings.FILE_TEMPLATE = "{docker_conatiner_name}/%Y/%m/%d_%H.%M.%S_{db_name}.tar"
    db_name = "test_db"
    docker_conatiner_name = "test_container"
    now = datetime(2021, 12, 25, 15, 2, 1)
    expected_tar_path = f"{docker_conatiner_name}/2021/12"
    expected_tar_name = f"25_15.02.01_{ db_name }.tar"

    ret = tar_file("test_container", "test_db", "/tmp/1234", now)
    assert ret, "tar_file failed"

    mock_run_tar.assert_called_once_with("tar", ["tar", "-cf", expected_tar_name, db_name], cwd="/tmp/1234")

    mock_rclone.assert_called_once_with(
        ["copy", f"/tmp/1234/{expected_tar_name}", f"/tmp/test_dir/{expected_tar_path}"]
    )


if __name__ == "__main__":
    test_tar_and_rclone_params()
