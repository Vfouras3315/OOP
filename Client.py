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
        """Метод для отправки данных на сервер"""
        timestamp = timestamp or int(time.time())
        try:
            self.conn.sendall(f'put {key} {value} {timestamp}\n'.encode())
        except socket.error as err:
            raise ClientError('error put', err)

        self.reader()

    def get(self, key):
        """Метод для получения данных с сервере"""
        try:
            self.conn.sendall(f'get {key}\n'.encode())
        except socket.error as err:
            raise ClientError('error get', err)

        dataset = self.reader()

        data = dict()
        if dataset == "":  # В задании просили:  если данных нет, то позвращать пустой словарь
            return data

        for row in dataset.split("\n"):  # итерируюсь по данным и присваиваю их к необходимым переменным
            try:
                key, value, timestamp = row.split()
            except ValueError as err:
                raise ClientError('this is sparta', err)
            if key not in data:
                data[key] = []
            data[key].append((int(timestamp), float(value)))

        return data  # возвращаем отсортированные данные, пока не указал как

    def reader(self):
        """Метод для чтения данных, полученых от сервера"""
        data = b""
        while not data.endswith(b"\n\n"):  # скачиваем ответ до '\n\n', это конец строки ответа
            try:
                data += self.conn.recv(1024)  # грузим их а data
            except socket.error as err:
                raise ClientError('download error', err)

        dec_data = data.decode('utf-8')  # декодируем bytes в utf-8
        status, dataset = dec_data.split("\n", 1)  # разбиваем полученный ответ на  status и dataset
        dataset = dataset.strip()

        if status == "error":  # если приходят не валидные данные, сервер присылает статус error
            raise ClientError('wrong command')

        return dataset

    def close(self):
        self.conn.close()


class ClientError(Exception):
    """Клиентская ошибка"""
    pass
