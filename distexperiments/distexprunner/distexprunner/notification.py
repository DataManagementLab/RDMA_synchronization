
import os
import json
import urllib.request
import logging


def get_user():
    if 'SUDO_USER' in os.environ:
        return os.environ['SUDO_USER']
    else:
        return os.environ['USER']


class Notifier:
    def send(self, message):
        pass

    def on_finish(self, num_exps):
        pass


class Slack(Notifier):
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url


    def send(self, message):
        # curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' <hook-url>
        data = {
            'text': message
        }

        req = urllib.request.Request(
            self.webhook_url,
            data=json.dumps(data).encode('utf8'),
            headers={
                'content-type': 'application/json'
            }
        )
        response = urllib.request.urlopen(req)
        logging.info(f'Slack API: {response.read().decode("utf8")}')


    def on_finish(self, num_exps):
        self.send(f'*_Status report_*\n\n*{num_exps}* experiments from *{get_user()}* finished.')