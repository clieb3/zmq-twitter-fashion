#!/usr/bin/env python

from __future__ import print_function

from collections import defaultdict

import zmq

TWEET_INGEST_URL = "tcp://127.0.0.1:5558"
CONTROL_URL = "tcp://127.0.0.1:5559"

def print_metrics(tweets_by_user, f):
    def count_tweets_by_users(f):
        for user, tweets in tweets_by_user.iteritems():
            print("%s: #%d tweets" % (user, len(tweets)), file=f)

    count_tweets_by_users(f)

def process_tweet(tweets_by_user, handle, text):
    tweets_by_user[handle].append(text)

    with open('tweet_aggregator.log', 'w') as f:
        print_metrics(tweets_by_user, f)

def tweet_aggregator():
    context = zmq.Context()

    # data-in
    tweets_receiver = context.socket(zmq.PULL)
    tweets_receiver.bind(TWEET_INGEST_URL)

    # control
    control_receiver = context.socket(zmq.SUB)
    control_receiver.connect(CONTROL_URL)
    control_receiver.setsockopt(zmq.SUBSCRIBE, "")

    poller = zmq.Poller()
    poller.register(tweets_receiver, zmq.POLLIN)
    poller.register(control_receiver, zmq.POLLIN)

    tweets_by_user = defaultdict(list)

    while True:
        try:
            socks = dict(poller.poll())

            if socks.get(tweets_receiver) == zmq.POLLIN:
                message = tweets_receiver.recv_json()
                handle = message['handle']

                try:
                    process_tweet(tweets_by_user, handle, message['text'])
                except Exception, e:
                    print(e, handle)

            elif socks.get(control_receiver) == zmq.POLLIN:
                control_message = control_receiver.recv()

                if control_message == "FINISHED":
                    with open('log.tweet_aggregator.txt', 'w') as f:
                        print("Received %s, printing metrics" % (control_message), file=f)

                        print_metrics(tweets_by_user, f)

                        print("Quitting", file=f)
                    break

        except Exception, e:
            print(e)
