from __future__ import annotations
import socket
import os
import time


SOCKET_FILE_PATH = '/tmp/statsd.sock'

def start(): 

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    # sock.bind(SOCKET_FILE_PATH)
    push_indef(sock)


def push_indef(sock: socket.socket): 

    interval_start = time.time()
    while True:
        now = time.time()
        if now - interval_start > 5:
            interval_start = now
            sock.sendto(b"Hello", SOCKET_FILE_PATH)
            print('sent')

        



if __name__ == "__main__":
    start()