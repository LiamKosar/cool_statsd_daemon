from __future__ import annotations
import socket
import os
from psycopg2 import pool
import time
from multiprocessing import Process, Manager
import logging
from queue import Queue, Empty
from contextlib import contextmanager
from threading import Thread
from settings import Config

# makes a log that looks like this:
# 2025-10-31 14:11:25 - INFO - worker 1 is processing 5 items
logging.basicConfig(
    filename='log_server.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def start_log_daemon(): 
    """
    I'm making two processes here just in case you can't write to postgres as fast as logs come in.
    The log_queue has NO MAX SIZE! this could nuke your memory usage, please do some careful monitoring!
    """
    with Manager() as manager:
        log_queue = manager.Queue()
        config = Config()
        config.initialize()
        p1 = Process(target=listener, args=(log_queue, config, ))
        p2 = Process(target=log_push_process, args=(log_queue, config, ))
    
        p1.start()
        p2.start()

        p1.join()
        p2.join()

def listener(log_queue: Queue[tuple[str, str, float]], config: Config):
    """
    Listening for UDP datagrams from port. Listening for MAX size on datagram data because
    I don't know how large a message should be.
    Messages received should be in byte string format.
    Putting these logs in a queue so they can be processed in parallel by queue workers
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", config.port))
    while True:
        data, _ = sock.recvfrom(config.datagram_max_size)
        msg = data.strip()
        metric_string = msg.decode('utf-8')
        name, value, timestamp = metric_string.split(":")
        timestamp = float(timestamp)
        log_queue.put((name, value, timestamp))

def log_push_process(log_queue: Queue[tuple[str, str, float]], config: Config):
    """
    Making a connection pool here to prevent constant opening/closing of connections across the worker threads.
    Hopefully this works at scale!
    """
    connection_pool = ConnectionPoolManager(os.environ.get(config.postgresql_env_variable_name), min_conn=config.min_connections, max_conn=config.max_connections)
    queue_worker_threads = []
    try:
        for i in range(config.num_queue_workers):
            t = Thread(target=log_queue_worker, args=(connection_pool, log_queue, i,))
            t.start()
            queue_worker_threads.append(t)
    except Exception as e:
        # graceful shutdown is not a garuntee. you might have some hanging postgres connections, but
        # they should die after a reasonable time
        logging.error(f"Shutting down gracefully after {e}")
        connection_pool.close_all()


def log_queue_worker(connection_pool: ConnectionPoolManager ,log_queue: Queue[tuple[str, str, float]], thread_num: int):
    logging.info(f'Starting log_queue_worker {thread_num}')
    batch = []
    last_flush = time.time()
    
    batch_timeout = Config().log_batch_timeout
    batch_size = Config().log_batch_size

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
    config = Config()
    try:
        with connection_pool.get_connection() as conn:
            with conn.cursor() as cursor:
                # Your SQL implementation here
                # Example framework:
                cursor.executemany(
                    f"INSERT INTO {config.log_table_name} (name, value, timestamp) VALUES (%s, %s, %s)",
                    batch
                )
                
                # For now, just log that we would process the batch
                logging.info(f"Processing batch of {len(batch)} items")             
    except Exception as e:
        logging.error(f"Failed to process batch: {e}, {batch=}")


class ConnectionPoolManager:
    """Manages the psycopg2 connection pool"""
    
    def __init__(self, dsn: str, min_conn: int, max_conn: int):
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
    start_log_daemon()