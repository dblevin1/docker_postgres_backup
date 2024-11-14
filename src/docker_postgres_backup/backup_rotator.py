import subprocess
import json
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import relativedelta
from .config import log, settings
from .rclone_manager import run_rclone
import os


def do_db_backup_file_rotation():
    if not settings.STAGGERED_ROTATOR:
        log.debug("Staggered rotation disabled")
        return
    args = [
        "lsjson",
        "-R",
        "--files-only",
        settings.BACKUP_LOCATION,
    ]
    got = run_rclone(args)
    if isinstance(got, bool) or not got.strip():
        log.warning(f"Failed to get list of files to rotate: {got=}")
        return

    gotjson = json.loads(got)
    rotator = StaggeredFileRotator()
    to_delete = []
    for db_name in settings.DB_NAMES:
        rotator.load_folder(gotjson, name_filter=db_name)
        to_delete.extend(rotator.to_delete)
    if settings.TEST_ROTATOR:
        msgstr = "---------Test Rotator Results---------\n"
        msgstr += ("Hourly:\n" + "\n".join([f" '{k}': '{v}'" for k, v in rotator.hourly.items()])).strip() + "\n"
        msgstr += ("Daily:\n" + "\n".join([f" '{k}': '{v}'" for k, v in rotator.daily.items()])).strip() + "\n"
        msgstr += ("Monthly:\n" + "\n".join([f" '{k}': '{v}'" for k, v in rotator.monthly.items()])).strip() + "\n"
        msgstr += ("Yearly:\n" + "\n".join([f" '{k}': '{v}'" for k, v in rotator.yearly.items()])).strip() + "\n"
        msgstr += ("To Delete:\n" + "\n".join([f" '{v}'" for v in to_delete])).strip()
        log.warning(msgstr)
        return to_delete

    if settings.DRY_RUN_ROTATOR:
        log.info(
            f"Dry Run, would've deleted {len(to_delete)} files, run with --test-rotator to see what would've been deleted"
        )
        return

    for item in to_delete:
        # item is a relative path to settings.BACKUP_LOCATION
        log.debug(f"Deleting: {item}")
        fullpath = os.path.join(settings.BACKUP_LOCATION, item)
        args = ["delete", fullpath, "--rmdirs"]
        run_rclone(args)

    if to_delete:
        log.info(f"Deleted {len(to_delete)} files")


class StaggeredFileRotator:
    def load_folder(self, rclone_json, name_filter=""):
        """
        Keeps:
            1 file per year
            1 file per month for the current year
            1 file per day for the current month
            1 file per hour for the current day
        """
        now = dt.now()
        self.hourly = {}
        self.daily = {}
        self.monthly = {}
        self.yearly = {}
        self.to_delete = []

        for item_dict in rclone_json:
            if name_filter not in item_dict["Name"]:
                continue
            item = item_dict["Path"]
            mod_time = item_dict["ModTime"].replace("Z", "")
            mod_time = dt.fromisoformat(mod_time)
            hourly_date_str = mod_time.strftime("%Y-%m-%d %H")
            daily_date_str = mod_time.strftime("%Y-%m-%d")
            monthly_date_str = mod_time.strftime("%Y-%m")
            yearly_date_str = mod_time.strftime("%Y")

            if mod_time > now - relativedelta(days=1) and hourly_date_str not in self.hourly:
                self.hourly[hourly_date_str] = item
            elif mod_time > now - relativedelta(months=1) and daily_date_str not in self.daily:
                self.daily[daily_date_str] = item
            elif mod_time > now - relativedelta(years=1) and monthly_date_str not in self.monthly:
                self.monthly[monthly_date_str] = item
            elif mod_time < now - relativedelta(years=1) and yearly_date_str not in self.yearly:
                self.yearly[yearly_date_str] = item
            else:
                self.to_delete.append(item)
        return


if __name__ == "__main__":
    do_db_backup_file_rotation()
