from __future__ import annotations
import socket
import time

"""
datagram structure
<METRIC_NAME>:<VALUE>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
"""

PORT = 8125

def start(): 

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    push_indef(sock)


def push_indef(sock: socket.socket): 

    interval_start = time.time()
    cool_str = f'name:value:{interval_start}'.encode('utf-8')
    while True:
        now = time.time()
        if now - interval_start > 1:
            interval_start = now
            sock.sendto(cool_str, ("127.0.0.1", PORT))
            print('sent')

if __name__ == "__main__":
    start()