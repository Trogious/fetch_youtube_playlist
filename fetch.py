import hashlib
import json
import logging
import os
import sqlite3
import subprocess
from logging.handlers import RotatingFileHandler

import googleapiclient.discovery
import googleapiclient.errors
from notify import Notify

CONFIG_PATH = os.getenv("FETCH_CONFIG_PATH")
FB_PAGE_ACCESS_TOKEN = os.getenv("FETCH_FB_PAGE_ACCESS_TOKEN")
FB_RECIPIENT_ID = os.getenv("FETCH_FB_RECIPIENT_ID")
COOKIES_PATH = os.path.join(CONFIG_PATH[:CONFIG_PATH.rfind("/")], "cookies.txt")


class Logger:
    logger = None

    @staticmethod
    def get_logger(path=os.getenv("FETCH_LOG_PATH", "./fetch.log"), level=logging.INFO, max_bytes=204800, backup_count=4):
        if Logger.logger is None:
            Logger.logger = logging.getLogger("fetch_logger")
            Logger.logger.setLevel(level)
            handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count)
            handler.setFormatter(logging.Formatter("%(asctime)s|%(levelname)s|%(lineno)d|%(message)s", "%Y-%m-%d %H:%M:%S"))
            Logger.logger.addHandler(handler)
            # logging.getLogger("asyncio").addHandler(handler)
        return Logger.logger


class Fetch:
    def __init__(self):
        self.con = None
        self.logger = Logger.get_logger()
        with open(CONFIG_PATH, "r") as cfg:
            self.config = json.load(cfg)
        self.notify = Notify(self.logger, FB_PAGE_ACCESS_TOKEN, FB_RECIPIENT_ID)

    def store_id(self, video_id):
        cur = self.con.cursor()
        cur.execute("INSERT INTO fetched (video_id) VALUES(?)", (video_id,))

    def fetch_video(self, video_id, out_dir, args, i):
        order_i = "%03d_" % i if i > 0 else ""
        cmd = [self.config["binary"]] + args + ["--cookies", COOKIES_PATH, "-o", out_dir + order_i + "%(title)s.%(ext)s", "--", video_id]
        self.logger.info(" ".join(cmd))
        fetched = False
        try:
            ret = subprocess.run(cmd, check=True)
            if ret.returncode == 0:
                fetched = True
            else:
                self.logger.error("subprocess return code: %d" % ret.returncode)
        except Exception as e:
            self.logger.error(e, exc_info=e)
        return fetched

    def process_item(self, item, playlist, i=0):
        refetch = "refetch" in playlist and playlist["refetch"]
        video_id = item["snippet"]["resourceId"]["videoId"]
        video_title = item["snippet"]["title"]

        if "title_match" in playlist and playlist["title_match"]:
            title_match = playlist["title_match"].lower()
            if title_match not in video_title.lower():
                return

        try:
            with self.con:
                if not refetch:
                    self.store_id(video_id)
                if self.fetch_video(video_id, playlist["output_dir"], playlist["args"], i):
                    if "notify" in playlist and playlist["notify"]:
                        self.notify.fb_send(video_title)
                else:
                    self.con.rollback()
        except sqlite3.IntegrityError:
            self.logger.info("already fetched: " + video_id)
        except sqlite3.OperationalError as e:
            raise e

    def main(self):
        playlist_output_dirs = set()
        for playlist in self.config["playlists"]:
            playlist_output_dirs.add(playlist["output_dir"])
            youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=self.config["api_key"])
            request = youtube.playlistItems().list(part="snippet", playlistId=playlist["id"], maxResults=50)
            response = request.execute()

            self.con = sqlite3.connect(self.config["db_file"])
            i = 1
            while request is not None:
                response = request.execute()
                for item in response["items"]:
                    if "enumerate" in playlist and playlist["enumerate"]:
                        self.process_item(item, playlist, i)
                    else:
                        self.process_item(item, playlist)
                    i += 1
                if "entire_playlist" not in playlist or not playlist["entire_playlist"]:
                    break
                request = youtube.playlistItems().list_next(request, response)
            self.con.close()
        for output_dir in playlist_output_dirs:
            self.remove_orphan_sha1_files(output_dir)

    def write_cksum(self, file_path):
        cksum = None
        with open(file_path, "rb") as f:
            BUF_LEN = 1024 * 1024 * 64
            b = bytearray(BUF_LEN)
            cksum = hashlib.sha1()
            n = 1
            while n > 0:
                n = f.readinto(b)
                if n == BUF_LEN:
                    cksum.update(b)
                elif n > 0:
                    cksum.update(b[:n])
        if cksum:
            cksum = cksum.hexdigest()
            try:
                with open(file_path + ".sha1", "w") as f:
                    f.write(cksum)
            except Exception as e:
                self.logger.error(e, exc_info=e)
        else:
            self.logger.warning("cannot get sha1 for: %s" % file_path)
        return cksum

    def remove_orphan_sha1_files(self, dir_path):
        for filename in os.listdir(dir_path):
            self.logger.info(f"Checking for orphan SHA1 file: {filename}")
            if filename.endswith('.sha1'):
                original = filename[:-5]  # remove ".sha1"
                self.logger.info(f"Checking for original file: {original}")
                if not os.path.isfile(original):
                    self.logger.info(f"Deleting orphan SHA1 file: {filename}")
                    os.remove(filename)


if __name__ == "__main__":
    Fetch().main()
