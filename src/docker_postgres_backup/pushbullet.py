import requests
import json
from .config import settings, log, BaseNotifier


class Pushbullet(BaseNotifier):
    def __init__(self, subject, maxsize=500):
        self.key = settings.PUSHBULLET.get_secret_value()
        self.subject = subject
        self.maxsize = maxsize * 1024

    def send(self, msg: str):
        if not self.key:
            log.warning("Failed to send pushbullet because api key is not set")
            return

        try:
            if self.maxsize and len(msg) > self.maxsize:
                cut_lines = msg.count("\n", self.maxsize // 2, -self.maxsize // 2)
                msg = (
                    "NOTE: Log was too big for pushbullet and was shortened\n\n"
                    + msg[: self.maxsize // 2]
                    + "[...]\n\n\n --- LOG WAS TOO BIG - {} LINES REMOVED --\n\n\n[...]".format(cut_lines)
                    + msg[-self.maxsize // 2 :]
                )
            data = {"type": "note", "title": self.subject, "body": msg}

            session = requests.Session()
            session.auth = (self.key, "")
            session.headers.update({"Content-Type": "application/json"})
            r = session.post("https://api.pushbullet.com/v2/pushes", data=json.dumps(data))
            if r.status_code != requests.codes.ok:
                raise Exception(f"Error calling pushbullet:{r.text}")
        except Exception as e:
            log.critical(f"Failed to send pushbullet: {type(e).__name__}:{e}")
