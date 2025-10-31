from __future__ import annotations
import socket
import os
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional
import re


"""
datagram structure
<METRIC_NAME>:<VALUE>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
"""

class StatType(Enum):
    GAUGE = 'g'
    COUNT = 'c'


@dataclass
class Metric:
    metric_name: str
    metric_value: int | float
    stat_type: StatType
    sample_rate: Optional[float] = None
    tags: Optional[Dict[str, str]] = None

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
        metric_string = msg.decode('utf-8')
        
        pattern = r'^([^:]+):([^|]+)\|([a-zA-Z]+)(?:\|@([\d.]+))?(?:\|#(.+))?$'

        match = re.match(pattern, metric_string)
        if not match:
            raise ValueError(f"Invalid metric format: {metric_string}")
        metric_name, metric_value, stat_type, sample_rate, tags_str = match.groups()

        try:
            metric_value = float(metric_value)
        except ValueError:
            raise ValueError(f"Invalid value: {metric_value}")
        
        # Parse optional sample rate
        if sample_rate:
            try:
                sample_rate = float(sample_rate)
            except ValueError:
                raise ValueError(f"Invalid sample rate: {sample_rate}")
        
        # Parse optional tags
        tags = None
        if tags_str:
            tags = {}
            for tag in tags_str.split(','):
                if ':' in tag:
                    key, val = tag.split(':', 1)  # Split only on first colon
                    tags[key] = val
                else:
                    # Handle tags without values (just keys)
                    tags[tag] = ''
        
        metric = Metric(metric_name, metric_value, stat_type, sample_rate, tags)
        print(metric)

if __name__ == "__main__":
    start()