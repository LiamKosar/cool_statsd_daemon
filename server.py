from __future__ import annotations
import socket
import os


SOCKET_FILE_PATH = '/tmp/statsd.sock'

def start(): 

    if os.path.exists(SOCKET_FILE_PATH):
        os.unlink(SOCKET_FILE_PATH)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.bind(SOCKET_FILE_PATH)
    listen_indef(sock)


def listen_indef(sock: socket.socket): 
    while True:
        data, _ = sock.recvfrom(65535)
        msg = data.strip()
        print(msg)

if __name__ == "__main__":
    start()