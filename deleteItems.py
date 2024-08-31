import logging
import os
import datetime
from logging.handlers import RotatingFileHandler

import googleapiclient.discovery
import googleapiclient.errors
from Google import Create_Service

CLIENT_SECRET_FILE = os.getenv('FETCH_API_KEY')
PLAYLIST_IDS = os.getenv('FETCH_PLAYLISTS').split(',')
FETCH_LOGGER_NAME = 'fetch_logger'


API_NAME  =  'youtube'
API_VERSION  =  'v3'
SCOPES  = ['https://www.googleapis.com/auth/youtube']


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


class DeleteItems:
    def __init__(self):
        self.logger = Logger.get_logger()

    def item_old_enough_to_delete(self, item):
        addedDateTimeStr = item['snippet']['publishedAt'].replace('Z','')
        added_date_time = datetime.datetime.fromisoformat(addedDateTimeStr)
        date_now = datetime.datetime.today()
        delta = date_now-added_date_time
        if delta.days > 30:
            self.logger.info('%d %s: %s' % (delta.days, item['snippet']['resourceId']['videoId'], item['snippet']['title']))
            return True
        return False
        #print(json.dumps(item, indent=2))

    def main(self):
        youtube =  Create_Service(CLIENT_SECRET_FILE, os.path.dirname(CLIENT_SECRET_FILE), API_NAME, API_VERSION, SCOPES)
        to_delete = []
        for i in range(len(PLAYLIST_IDS)):
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=PLAYLIST_IDS[i],
                maxResults=50
            )
            response = request.execute()

            while request is not None:
                response = request.execute()
                for item in response['items']:
                    if self.item_old_enough_to_delete(item):
                        to_delete.append(item['id'])
                request = youtube.playlistItems().list_next(request, response)

        for item_id in to_delete:
            try:
                youtube.playlistItems().delete(id=item_id).execute()
                self.logger.info('deleted: %s', item_id)
            except Exception as e:
                self.logger.error(e)


if __name__ == '__main__':
    DeleteItems().main()
