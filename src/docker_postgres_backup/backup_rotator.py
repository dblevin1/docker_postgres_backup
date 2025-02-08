import json
import os
from datetime import datetime as dt

from dateutil.relativedelta import relativedelta

from .config import log, settings
from .rclone_manager import run_rclone


def do_db_backup_file_rotation(docker_conatiner_name):
    if not settings.STAGGERED_ROTATOR:
        log.debug("Staggered rotation disabled")
        return
    log.info(f"Starting staggered rotation for '{docker_conatiner_name}'")
    folder_path = settings.BACKUP_LOCATION.format(docker_conatiner_name=docker_conatiner_name)
    args = [
        "lsjson",
        "-R",
        "--files-only",
        folder_path,
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
        msgstr = ("To Delete:\n" + "\n".join([f" '{v}'" for v in to_delete])).strip()
        log.debug(msgstr)
        log.info(f"Dry Run, would've deleted {len(to_delete)} files")
        return

    for item in to_delete:
        # item is a relative path to settings.BACKUP_LOCATION
        log.debug(f"Deleting: {item}")
        fullpath = os.path.join(folder_path, item)
        args = ["delete", fullpath]
        run_rclone(args)

    if to_delete:
        log.info(f"Deleted {len(to_delete)} files, Removing empty directories...")
        run_rclone(["rmdirs", folder_path, "--leave-root"])


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
    from .config import log, settings
    from .main import main

    settings.TEST_ROTATOR = True
    settings.OVERRIDE_CONTAINER_NAMES = ["happyacres-db-1", "blessesnest-db-1"]

    # sys.argv.append("--test-rotator")
    # sys.argv.append('--override_container_names="[happyacres-db-1, blessesnest-db-1]"')
    main()
