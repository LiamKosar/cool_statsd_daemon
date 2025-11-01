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

    while True:
        log_str = f'l:log_name:log_value:{time.time()}'.encode('utf-8')
        transaction_str = f't:transaction_name:transaction_value:{time.time()}'.encode('utf-8')
        sock.sendto(log_str, ("127.0.0.1", PORT))
        sock.sendto(transaction_str, ("127.0.0.1", PORT))
        print('sent')
        time.sleep(.02)
        


if __name__ == "__main__":
    start()