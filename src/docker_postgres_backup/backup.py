import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from typing import Optional

from .config import log, settings
from .rclone_manager import run_rclone


def _safe_run(breadcrumb: str, args, env: Optional[dict[str, str]] = None, cwd: Optional[str] = None) -> bool:
    kwargs = {}
    if env:
        kwargs["env"] = env
    if cwd:
        kwargs["cwd"] = cwd
    try:
        process = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            stderr=subprocess.PIPE,
            **kwargs,
        )
        if process.returncode != 0:
            log.error(f"'{breadcrumb}' failed: {process.stdout}{process.stderr}")
            return False
    except Exception as e:
        log.error(f"'{breadcrumb}' failed: {type(e).__name__}:{e}")
        return False
    return True


def run(docker_conatiner_name: str):
    """
    1. Docker exec to dump database to docker folder
    2. Move the dump to host
    3. Tar the dump
    4. Move the tar to backup location
    """
    now = datetime.now()

    for db_name in settings.DB_NAMES:
        log.info(f"Backing up database '{db_name}'")
        backup_dir = do_data_db_backup(docker_conatiner_name, db_name)
        if backup_dir:
            tar_file(docker_conatiner_name, db_name, backup_dir.name, now)
            backup_dir.cleanup()
    log.info("Finshed database backup")


def do_data_db_backup(docker_conatiner_name: str, db_name: str):
    db_user = settings.DB_USER
    db_pass = settings.DB_PASS.get_secret_value()
    db_host = settings.DB_HOST

    in_docker_folder = "/docker-entrypoint-initdb.d"

    # temp_dir = tempfile.TemporaryDirectory()
    # sql_file = os.path.join(temp_dir.name, db_name)
    custom_env = os.environ.copy()
    if db_pass:
        custom_env["PGPASSWORD"] = db_pass
    try:
        log.debug("Running pg_dump command...")

        # args = ["pg_dump", "-F", "c"]
        args = ["docker", "exec", "-t", docker_conatiner_name, "pg_dump", "-F", "c"]
        if db_user:
            args.extend(["-U", db_user])
        if db_host:
            args.extend(["-h", db_host])
        sql_file = os.path.join(in_docker_folder, db_name)
        args.extend(["-d", db_name, "-f", sql_file])

        if not _safe_run("pg_dump", args, custom_env):
            return False

        log.debug("Running docker cp command...")
        # sql_file is now in the docker container, we need to copy it to the host
        temp_dir = tempfile.TemporaryDirectory()
        args = ["docker", "cp", f"{docker_conatiner_name}:{sql_file}", temp_dir.name]
        if not _safe_run("docker cp", args):
            return False
        return temp_dir
    except Exception as e:
        log.error(f"Failed to backup database '{db_name}': {type(e).__name__}:{e}")


def tar_file(docker_conatiner_name, db_name, temp_dir_name, now=None):
    """Runs the tar command in the temp_dir_name
    and moves the tar file to the backup_location"""
    if not now:
        now = datetime.now()
    to_path = os.path.join(settings.BACKUP_LOCATION, settings.FILE_TEMPLATE)
    to_path = to_path.format(db_name=db_name, docker_conatiner_name=docker_conatiner_name)
    to_path = now.strftime(to_path)
    # tar_file_name = str(db_name) + ".tar"
    tar_file_name = os.path.basename(to_path)
    if not tar_file_name.endswith(".tar"):
        tar_file_name = tar_file_name + ".tar"
    to_path = os.path.dirname(to_path)
    # try:
    #     os.makedirs(os.path.dirname(to_path), exist_ok=True)
    # except Exception as e:
    #     log.error(f"Failed to create backup location: {type(e).__name__}:{e}")
    #     return False

    try:
        log.debug("Tar sql file...")
        args = ["tar", "-cf", tar_file_name, db_name]
        if not _safe_run("tar", args, cwd=temp_dir_name):
            return False

        log.debug("Moving tar file...")
        from_path = os.path.join(temp_dir_name, tar_file_name)
        run_rclone(["copy", from_path, to_path])

        # shutil.move(os.path.join(temp_dir_name, tar_file_name), to_path)
        return True
    except Exception as e:
        log.error(f"Failed to tar '{db_name}': {type(e).__name__}:{e}")
