#!/usr/local/bin/python
import json
import logging
import signal
import socket
import time

def signal_handler(signum, frame):
  # This will cause the exit handler to be executed, if it is registered.
  raise RuntimeError('Caught SIGTERM, exiting.')

def send_update(sock, status):
  message = json.dumps(status).encode('utf-8') + b'\n'
  sock.sendto(message, ('127.0.0.1', 2048))

logging.basicConfig(level=logging.DEBUG, format=f"%(asctime)s %(levelname)s: %(message)s")

signal.signal(signal.SIGTERM, signal_handler)

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

try:
  status = {
    'last_status_message': 'sleeping'
  }
  send_update(sock, status)

  for i in range(5):
    print(f"sleeping {i} ...")
    time.sleep(2)
    status = {
      'error_count': i,
      'expected_count': 5
    }
    send_update(sock, status)
    print("done sleeping")

  status = {
    'last_status_message': 'im woke'
  }
  send_update(sock, status)

finally:
  sock.close()