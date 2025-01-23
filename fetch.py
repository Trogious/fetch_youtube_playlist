import hashlib
import json
import logging
import os
import sqlite3
import subprocess
from logging.handlers import RotatingFileHandler

import googleapiclient.discovery
import googleapiclient.errors

CONFIG_PATH = os.getenv("FETCH_CONFIG_PATH")


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

    def store_id(self, video_id):
        cur = self.con.cursor()
        cur.execute("INSERT INTO fetched (video_id) VALUES(?)", (video_id,))

    def fetch_video(self, video_id, out_dir, args):
        cmd = [self.config["binary"]] + args + ["-o", out_dir + "%(title)s.%(ext)s", "--", video_id]
        self.logger.info(" ".join(cmd))
        try:
            with self.con:
                self.store_id(video_id)
                ret = subprocess.run(cmd, check=True)
                if ret.returncode != 0:
                    self.logger.error("subprocess return code: %d" % ret.returncode)
                    self.con.rollback()
        except sqlite3.IntegrityError:
            self.logger.info("already fetched: " + video_id)
        except sqlite3.OperationalError as e:
            raise e
        except Exception as e:
            self.logger.error(e, exc_info=e)

    def process_item(self, item, playlist):
        self.fetch_video(item["snippet"]["resourceId"]["videoId"], playlist["output_dir"], playlist["args"])

    def main(self):
        for playlist in self.config["playlists"]:
            youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=self.config["api_key"])
            request = youtube.playlistItems().list(part="snippet", playlistId=playlist["id"], maxResults=50)
            response = request.execute()

            self.con = sqlite3.connect(self.config["db_file"])
            while request is not None:
                response = request.execute()
                for item in response["items"]:
                    self.process_item(item, playlist)
                break
                request = youtube.playlistItems().list_next(request, response)
            self.con.close()

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


if __name__ == "__main__":
    Fetch().main()
