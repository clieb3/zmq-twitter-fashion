#!/usr/bin/env python

from __future__ import print_function

import time
import zmq

HANDLES = [
  '@RaySiegel',
  '@cindi_leive',
  '@shionat',
  '@WeWoreWhat',
  '@manrepellar', # page doesn't exist
  '@derekblasberg',
  '@KyleEditor',
  '@AByrneNotice',
  '@choupettesdiary',
  '@JaneKeltnerdeV',
  '@choitotheworld',
  '@ATCodinha']

HANDLE_INGEST_URL = "tcp://127.0.0.1:5557"

# alternatively could read from db, stream, web-server
def handle_generator():
    for handle in HANDLES:
        yield handle

def handle_ventilator():
    context = zmq.Context()
 
    handle_sender = context.socket(zmq.PUSH)
    handle_sender.bind(HANDLE_INGEST_URL)
 
    for handle in handle_generator():
        work_message = { 'handle' : handle }
        print(work_message)
        handle_sender.send_json(work_message)
 
    # wait for messages to be consumed
    time.sleep(5)
