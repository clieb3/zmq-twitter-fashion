#!/usr/bin/env python

from multiprocessing import Process
import signal
import sys
import time

import zmq

from handle_ventilator import handle_ventilator
from handle_ingester import handle_ingester
from tweet_aggregator import tweet_aggregator

CONTROL_URL = "tcp://127.0.0.1:5559"

def control_message(control_sender, topic):
    print("CONTROL:%s" % topic)
    control_sender.send(topic)

def cleanup_pipeline(control_sender):
    print("CONTROL:FINISHED")
    control_sender.send("FINISHED")

def sigint_handler(control_sender):
    def the_handler(signal, frame):
        cleanup_pipeline(control_sender)
        sys.exit(0)

    return the_handler

def main():
    context = zmq.Context()

    control_sender = context.socket(zmq.PUB)
    control_sender.connect(CONTROL_URL)

    signal.signal(signal.SIGINT, sigint_handler(control_sender))

    # for push/pull setup order doesn't matter (i think)
    # for pub/sub messages are lost until subscriber is

    # in: handle_ventilator
    # out: tweet_aggregator
    num_handle_ingesters = 5
    for i in range(num_handle_ingesters):
        Process(target=handle_ingester, args=()).start()

    # in: handle_ingesters
    # out: print
    Process(target=tweet_aggregator, args=()).start()

    # Give everything a second to spin up and connect
    time.sleep(1)

    # Lost message ?
    control_message(control_sender, "STARTING")

    # in: None
    # out: handle_ingesters
    Process(target=handle_ventilator, args=()).start()

    num_cyles = 5
    for i in range(num_cyles):
        time.sleep(1)

    cleanup_pipeline(control_sender)

if __name__ == '__main__':
    main()
