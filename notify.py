import json
import requests


class Notify:
    def __init__(self, logger, fb_page_access_token, fb_recipient_id):
        self.log = logger
        self.FB_PAGE_ACCESS_TOKEN = fb_page_access_token
        self.FB_RECIPIENT_ID = fb_recipient_id

    def _fb_send_to(self, recipient_id, message):
        params = {"access_token": self.FB_PAGE_ACCESS_TOKEN}
        headers = {"Content-Type": "application/json"}
        data = json.dumps({"recipient": {"id": recipient_id}, "message": {"text":  message},
                           "messaging_type": "MESSAGE_TAG", "tag": "ACCOUNT_UPDATE"})
        r = requests.post("https://graph.facebook.com/v6.0/me/messages", params=params, headers=headers, data=data)
        if r.status_code != 200:
            self.log.error('fb_send_to: %d %s' % (r.status_code, r.text))

    def fb_send(self, message):
        maxMsgLen = 640
        recipientId = self.FB_RECIPIENT_ID
        while len(message) > maxMsgLen:
            idx = message.rfind("\n", 0, maxMsgLen)
            if idx >= 0:
                msg = message[:idx]
                message = message[idx+1:]
                self._fb_send_to(recipientId, msg)
        self._fb_send_to(recipientId, message)
