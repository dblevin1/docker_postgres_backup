import os
import shutil
import subprocess
from .config import log, settings

DID_UPDATE_CHECK = False


def run_rclone(args: list[str]) -> bool | str:
    """dont include rclone in the args"""
    ensure_rclone_installed()
    assert len(args) > 1, "rclone command missing"
    breadcrumb = f"rclone {args[0]}"
    log.debug(f"Running {breadcrumb}...")
    config_file = os.path.expanduser(settings.RCLONE_CONFIG_PATH)
    args = [settings.RCLONE_BINARY_PATH, "--config", config_file] + args
    kwargs = {}
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
        return process.stdout or True
    except Exception as e:
        log.error(f"'{breadcrumb}' failed: {type(e).__name__}:{e}")
        return False


def ensure_rclone_installed():
    global DID_UPDATE_CHECK
    if DID_UPDATE_CHECK:
        return
    DID_UPDATE_CHECK = True

    rclone_path = settings.RCLONE_BINARY_PATH
    if not os.path.isfile(rclone_path):
        if not settings.RCLONE_AUTO_UPDATE:
            raise FileNotFoundError(f"rclone binary not found at {rclone_path} and auto update is disabled")
        install_rclone()
    elif settings.RCLONE_AUTO_UPDATE:
        proc = subprocess.run([rclone_path, "selfupdate"], capture_output=True)
        if "rclone is up to date" in str(proc):
            return
        else:
            # did an update or had an error
            log.warning(f"rclone selfupdate: {proc.stdout.decode()}{proc.stderr.decode()}")
        if proc.returncode != 0:
            log.error(f"Failed to update rclone: {proc.stderr.decode()}")


def install_rclone():
    rclone_path = settings.RCLONE_BINARY_PATH
    log.info("Installing rclone...")
    proc = subprocess.run(
        [
            "curl",
            "-o",
            "/tmp/rclone.zip",
            "https://downloads.rclone.org/rclone-current-linux-amd64.zip",
        ],
        capture_output=True,
    )
    log.debug(str(proc))
    proc = subprocess.run(["unzip", "-j", "-d", "/tmp/rclone", "/tmp/rclone.zip"], capture_output=True)
    log.debug(str(proc))
    proc = subprocess.run(["cp", "/tmp/rclone/rclone", rclone_path], capture_output=True)
    log.debug(str(proc))
    proc = subprocess.run(["chmod", "+x", "/mnt/rclone"], capture_output=True)
    log.debug(str(proc))
    shutil.rmtree("/tmp/rclone")
