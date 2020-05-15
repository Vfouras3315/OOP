import asyncio
import pdb


class Storage:

    def __init__(self):
        self.data = {}

    def put(self, key, value, timestamp):
        if key not in self.data:
            self.data[key] = {}
        self.data[key] = timestamp, value

    def get(self, key):
        if key != '*':
            self.data = {key: self.data.get(key)}

        # тут будет сортировка

        return self.data



class Protocol(asyncio.Protocol):

    def connection_made(self, transport):
        """Вызывается при установлении соединения.
        Аргумент - это транспорт, представляющий соединение канала.
        Чтобы получить данные, дождитесь вызовов data_received ()."""

        self.transport = transport


    def data_received(self, data):
        """Вызывается когда поступают данные
        Аргументы принемаются в байтах"""

        while not data.endswith(b'\n'):
            accum_data += data

        command = self.decode(data)
        resp = self.interface_storage(command)
        self.transport.write(self.encode(resp))



    def interface_storage(self, result_list):

        if result_list[0] == 'put':
            del result_list[0]
            result = Storage.put(result_list)
        elif result_list[1] == 'get':
            del result_list[0]
            result = Storage.get(result_list)

        return result

    def decode(self, data):
        decode_data = data.decode('utf-8')
        command, dataset = data.split(" ", 1)
        result_list = []
        if command == 'put':
            key, value, timestamp = dataset.split()
            result_list.append(command, key, value, timestamp)
        elif command == 'get':
            key = dataset
            result_list.append(command, key)
        else:
            raise ValueError

        return result_list

    def encode(self, resp):
        set = []

        for response in resp:
            if not response:
                continue
            for key, values in response.items():
                for timestamp, value in values:
                    set.append(f"{key} {value} {timestamp}")

        result = "ok\n"

        if set:
            result += "\n".join(set) + "\n"

        return (result + "\n").encode()


def run_server(host, port):

    loop = asyncio.get_event_loop()  # получаем event loop
    coro = asyncio.create_server(Protocol, host, port)  # запускаем корутину на адрес
    server = loop.run_until_complete(coro)  # выполняем пока не завершится
    try:
        loop.run_forever()  # обрабатываем все входящие соединения и запускаем для каждого корутину
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_close())
    loop.close()


if __name__ is '__main__':
    pdb.set_trace()
    run_server('127.0.0.1', 8888)
