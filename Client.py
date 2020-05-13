import socket
import time
from operator import itemgetter


class Client:
    def __init__(self, host, port, timeout=None):
        self.addr = host
        self.port = port
        try:
            self.conn = socket.create_connection((host, port), timeout)
        except socket.error as err:
            raise ClientError('connection error', err)

    def put(self, key, value, timestamp=None):
        timestamp = timestamp or int(time.time())
        try:
            self.conn.sendall(f'put {key} {value} {timestamp}\n'.encode())
        except socket.error as err:
            raise ClientError('error put', err)

        self.reader()

    def get(self, key):
        try:
            self.conn.sendall(f'get {key}\n'.encode())
        except socket.error as err:
            raise ClientError('error get', err)

        dataset = self.reader()

        data = {}
        if dataset == "":
            return data

        for row in dataset.split("\n"):
            key, value, timestamp = row.split()
            if key not in data:
                data[key] = []
            data[key].append((int(timestamp), float(value)))

        return data

    def reader(self):
        data = b""
        while not data.endswith(b"\n\n"):
            try:
                data += self.conn.recv(1024)
            except socket.error as err:
                raise ClientError('download error', err)

        dec_data = data.decode('utf-8')
        status, dataset = dec_data.split("\n", 1)

        if status == "error":
            raise ClientError('wrong command')

        return dataset

    def close(self):
        self.conn.close()


class ClientError(Exception):
    pass
