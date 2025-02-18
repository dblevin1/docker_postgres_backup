import logging
from abc import ABC, abstractmethod
from typing import Literal
import os
import dotenv
from pydantic import SecretStr, PrivateAttr, Field
from pydantic_settings import BaseSettings, CliApp, CliImplicitFlag
from rich.logging import RichHandler
from logging.handlers import TimedRotatingFileHandler

dotenv.load_dotenv(os.path.join(os.getcwd(), ".env"))


class BaseNotifier(ABC):
    @abstractmethod
    def send(self, record: str): ...


class Settings(BaseSettings):
    DB_IMAGE_NAME: str = "postgres"
    DB_NAMES: list[str] = ["data", "docassemble"]
    DPB_LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "DEBUG"
    DPB_LOG_FILE: str | None = None
    DPB_NICENESS: int = 15
    DB_USER: str = "postgres"
    DB_PASS: SecretStr = SecretStr("")
    DB_HOST: str = ""

    BACKUP_LOCATION: str = "/tmp/backups/test_{docker_conatiner_name}"
    """ Backup rotation will be done on this location """

    FILE_TEMPLATE: str = "%Y/%m/%d_%H.%M.%S_{db_name}.tar"
    """ first pased through python str format, then through dt.strftime"""

    PUSHBULLET: SecretStr = SecretStr("")

    RCLONE_CONFIG_PATH: str = "~/.config/rclone/rclone.conf"
    RCLONE_BINARY_PATH: str = "/usr/bin/rclone"
    RCLONE_AUTO_UPDATE: CliImplicitFlag[bool] = False

    OVERRIDE_CONTAINER_NAMES: list[str] | None = None
    TEST_ROTATOR: CliImplicitFlag[bool] = False
    DRY_RUN_ROTATOR: CliImplicitFlag[bool] = False
    """ if true, will do everything except delete files """
    STAGGERED_ROTATOR: CliImplicitFlag[bool] = True

    # -- Private Attributes --
    _notifiers: list[BaseNotifier] = PrivateAttr(default_factory=list)

    def init(self, notifiers: list[BaseNotifier]):
        self._notifiers = notifiers
        if settings.DPB_NICENESS != 0:
            # important note: new val = current niceness + DPB_NICENESS
            os.nice(settings.DPB_NICENESS)


class ErrorNotifyFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # only log errors, not warnings, infos
        # criticals should be used on the notification handler to prevent revursive notifications
        if record.levelno == logging.ERROR:
            for notifier in settings._notifiers:
                notifier.send(record.getMessage())

        return super().filter(record)


def setup_logging(settings: Settings):
    if settings.DPB_LOG_FILE:
        log_file = os.path.expanduser(settings.DPB_LOG_FILE)
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        # log_handler = logging.FileHandler(log_file)
        log_handler = TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            backupCount=7,
            encoding=None,
            delay=True,
        )
        log_fmt = "{levelname:9} {name:20} {asctime:25} {message}"
    else:
        log_fmt = "{name:20} {asctime:25} {message}"
        log_handler = RichHandler(rich_tracebacks=False, markup=True, show_time=False)
    formatter = logging.Formatter(log_fmt, style="{")
    log_handler.setFormatter(formatter)

    # Package logger
    log = logging.getLogger((__package__ or "").split(".")[0])
    log.addHandler(log_handler)
    log.addFilter(ErrorNotifyFilter())
    log.setLevel(settings.DPB_LOG_LEVEL)
    return log


settings = CliApp.run(Settings)
log = setup_logging(settings)
log.debug(f"Settings: {settings}")
# notifiers: list[BaseNotifier] = []
