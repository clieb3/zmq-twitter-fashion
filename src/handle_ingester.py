#!/usr/bin/env python

from __future__ import print_function

import time
import twitter
import zmq

# Get your own authentication keys
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN_KEY = ''
ACCESS_TOKEN_SECRET = ''

LANGUAGES = ['en']

HANDLE_INGEST_URL = "tcp://127.0.0.1:5557"
TWEET_INGEST_URL = "tcp://127.0.0.1:5558"
CONTROL_URL = "tcp://127.0.0.1:5559"

# alternatively could read from db
def stream_tweets(api, handle, num_tweets=5):
    # results in unauthorizd access
    # for i, status in enumerate(api.GetStreamFilter(track=[handle], languages=LANGUAGES)):
    #     if i >= num_tweets:
    #         break

    #     yield {
    #         'handle' : handle,
    #         'text' : status['text']
    #     }

    print("Processing handle %s" % handle)

    for status in api.GetUserTimeline(screen_name=handle, count=num_tweets):
        yield {
            'handle' : handle,
            'text' : status.text
        }
        time.sleep(0.1)

def handle_ingester():
    context = zmq.Context()

    # data-in
    handle_receiver = context.socket(zmq.PULL)
    handle_receiver.connect(HANDLE_INGEST_URL)

    # data-out
    tweets_sender = context.socket(zmq.PUSH)
    tweets_sender.connect(TWEET_INGEST_URL)

    # control
    control_receiver = context.socket(zmq.SUB)
    control_receiver.connect(CONTROL_URL)
    control_receiver.setsockopt(zmq.SUBSCRIBE, "")

    poller = zmq.Poller()
    poller.register(handle_receiver, zmq.POLLIN)
    poller.register(control_receiver, zmq.POLLIN)

    # reusable api handle
    api = twitter.Api(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token_key=ACCESS_TOKEN_KEY,
        access_token_secret=ACCESS_TOKEN_SECRET)

    while True:
        try:
            socks = dict(poller.poll())

            if socks.get(handle_receiver) == zmq.POLLIN:
                # handle_receiver.recv_multipart()
                message = handle_receiver.recv_json()
                handle = message['handle']

                try:
                    for tweet in stream_tweets(api, handle):
                        tweets_sender.send_json(tweet)
                except Exception, e:
                    print(e, handle)

            elif socks.get(control_receiver) == zmq.POLLIN:
                control_message = control_receiver.recv()
                if control_message == "FINISHED":
                    with open('log.handle_ingester.txt', 'a') as f:
                        print("Received %s, quitting!" % (control_message), file=f)
                    break

        except Exception, e:
            print(e)

