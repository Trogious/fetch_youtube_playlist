import logging
import os
import sqlite3
import subprocess
from logging.handlers import RotatingFileHandler

import googleapiclient.discovery
import googleapiclient.errors

API_KEY = os.getenv('FETCH_API_KEY')
PLAYLIST_ID = os.getenv('FETCH_PLAYLIST')
DB_FILE = os.getenv('FETCH_DB_FILE')
OUTDIR = os.getenv('FETCH_OUTDIR', './')
FETCH_LOGGER_NAME = 'fetch_logger'
CMD = ['youtube-dl', '--restrict-filenames', '-f', 'best', '-o', OUTDIR + '%(title)s.%(ext)s']


class Logger:
    logger = None

    @staticmethod
    def get_logger(path=os.getenv('FETCH_LOG_PATH', './fetch.log'), level=logging.INFO, max_bytes=204800,
                   backup_count=4):
        if Logger.logger is None:
            Logger.logger = logging.getLogger(FETCH_LOGGER_NAME)
            Logger.logger.setLevel(level)
            handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s|%(levelname)s|%(lineno)d|%(message)s', '%Y-%m-%d %H:%M:%S'))
            Logger.logger.addHandler(handler)
            # logging.getLogger("asyncio").addHandler(handler)
        return Logger.logger


class Fetch:
    def __init__(self):
        self.con = None
        self.logger = Logger.get_logger()

    def store_id(self, video_id):
        cur = self.con.cursor()
        cur.execute('INSERT INTO fetched (video_id) VALUES(?)', (video_id, ))

    def fetch_video(self, video_id):
        url = 'https://www.youtube.com/watch?v=' + video_id
        cmd = CMD + [url]
        pcmd = cmd[:]
        pcmd[-2] = "'" + pcmd[-2] + "'"
        pcmd[-1] = "'" + pcmd[-1] + "'"
        self.logger.info(' '.join(pcmd))
        try:
            with self.con:
                self.store_id(video_id)
                ret = subprocess.run(cmd, check=True)
                if ret.returncode != 0:
                    self.logger.error('subprocess return code: %d' % ret.returncode)
                    self.con.rollback()
        except sqlite3.IntegrityError as e:
            self.logger.info('already fetched: ' + video_id)
        except Exception as e:
            self.logger.error(e, exc_info=e)

    def process_item(self, item):
        # print('%s: %s' % (item['snippet']['resourceId']['videoId'], item['snippet']['title']))
        self.fetch_video(item['snippet']['resourceId']['videoId'])

    def main(self):
        youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)
        request = youtube.playlistItems().list(
            part='snippet',
            playlistId=PLAYLIST_ID,
            maxResults=50
        )
        response = request.execute()

        self.con = sqlite3.connect(DB_FILE)
        while request is not None:
            response = request.execute()
            for item in response['items']:
                self.process_item(item)
            break
            # request = youtube.playlistItems().list_next(request, response)
        self.con.close()


if __name__ == '__main__':
    Fetch().main()
