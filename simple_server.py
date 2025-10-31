from __future__ import annotations
import socket
import os
import psycopg2
from psycopg2 import pool
import time
from multiprocessing import Process, Manager
from functools import wraps
import logging
from queue import Queue, Empty
import logging
from contextlib import contextmanager
from threading import Thread

logging.basicConfig(
    filename='log_server.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

POSTGRESQL_URL = os.environ.get('STAT_DAEMON_POSTGRESQL_URL')
PORT = 8125
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 20

def start(num_queue_workers: int = 2): 
    with Manager() as manager:
        log_queue = manager.Queue()

        p1 = Process(target=listener, args=(log_queue,))
        p2 = Process(target=log_push_process, args=(log_queue, num_queue_workers,))

        p1.start()
        p2.start()

        p1.join()
        p2.join()

def listener(log_queue: Queue[tuple[str, str, float]]):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", PORT))
    while True:
        data, _ = sock.recvfrom(65535)
        msg = data.strip()
        metric_string = msg.decode('utf-8')
        name, value, timestamp = metric_string.split(":")
        timestamp = float(timestamp)
        log_queue.put((name, value, timestamp))

def log_push_process(log_queue: Queue[tuple[str, str, float]], num_queue_workers: int):
    connection_pool = ConnectionPoolManager(POSTGRESQL_URL, min_conn=num_queue_workers)
    queue_worker_threads = []
    try:
        for i in range(num_queue_workers):
            t = Thread(target=log_queue_worker, args=(connection_pool, log_queue, i,))
            t.start()
            queue_worker_threads.append(t)
    except Exception as e:
        logging.error(f"Shutting down gracefully after {e}")
        connection_pool.close_all()


def log_queue_worker(connection_pool: ConnectionPoolManager ,log_queue: Queue[tuple[str, str, float]], thread_num: int):
    logging.info(f'Starting log_queue_worker {thread_num}')
    # this function should open a cursor, and commit a log on loop.
    batch = []
    last_flush = time.time()
    
    batch_timeout = 100
    batch_size = 5

    while True:
        try:
            # Try to get an item with timeout for batch processing
            item = log_queue.get(timeout=0.1)
            batch.append(item)
            
            # Check if we should flush the batch
            if len(batch) >= batch_size or (time.time() - last_flush) > batch_timeout:
                if batch:
                    logging.info(f'worker {thread_num} is processing {len(batch)} items')
                    process_batch(connection_pool, batch)
                    batch = []
                    last_flush = time.time()              
        except Empty:
            # Queue is empty, check if we need to flush based on timeout
            if batch and (time.time() - last_flush) > batch_timeout:
                logging.info(f'worker {thread_num} is processing {len(batch)} items')
                process_batch(connection_pool, batch)
                batch = []
                last_flush = time.time()

def process_batch(connection_pool: ConnectionPoolManager, batch: list[tuple[str, str, float]]):
    try:
        with connection_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                # Your SQL implementation here
                # Example framework:
                cursor.executemany(
                    "INSERT INTO name_value_logs (name, value, timestamp) VALUES (%s, %s, %s)",
                    batch
                )
                
                # For now, just log that we would process the batch
                logging.info(f"Processing batch of {len(batch)} items")             
    except Exception as e:
        logging.error(f"Failed to process batch: {e}, {batch=}")


class ConnectionPoolManager:
    """Manages the psycopg2 connection pool"""
    
    def __init__(self, dsn: str, min_conn: int = MIN_CONNECTIONS, max_conn: int = MAX_CONNECTIONS):
        self.pool = None
        self.dsn = dsn
        self.min_conn = min_conn
        self.max_conn = max_conn
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        try:

            self.pool = pool.ThreadedConnectionPool(
                self.min_conn,
                self.max_conn,
                self.dsn
            )
            logging.info(f"Connection pool initialized with {self.min_conn}-{self.max_conn} connections")
        except Exception as e:
            logging.error(f"Failed to initialize connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection from the pool"""
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
            # If we get here, commit the transaction
            conn.commit()
        except Exception as e:
            # Rollback on any error
            if conn:
                conn.rollback()
            logging.error(f"Database operation failed: {e}")
            raise
        finally:
            # Always return connection to pool
            if conn:
                self.pool.putconn(conn)
    
    def close_all(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            logging.info("All connections closed")

if __name__ == "__main__":
    start()