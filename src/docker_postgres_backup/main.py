import subprocess

from . import backup
from .backup_rotator import do_db_backup_file_rotation
from .config import log, settings
from .pushbullet import Pushbullet


def get_db_container_names():
    # docker ps -f "ancestor=postgres" --format='{{ .Names }}'
    proc = subprocess.run(
        ["docker", "ps", "-f", "ancestor=postgres", "--format='{{ .Names }}'"], check=True, stdout=subprocess.PIPE
    )
    to_ret = proc.stdout.decode().replace("'", "").strip().split("\n")
    log.debug(f"Found containers: {to_ret}")
    return to_ret


def main():
    try:
        settings.init([Pushbullet("Docker Postgres Backup")])
        containers = settings.OVERRIDE_CONTAINER_NAMES or get_db_container_names()
        for container in containers:
            if not settings.TEST_ROTATOR:
                backup.run(container)
            do_db_backup_file_rotation(container)
    except Exception as e:
        log.exception(f"Unhandled {type(e).__name__}:{e}", exc_info=e)


if __name__ == "__main__":
    main()
