import asyncio
import pdb

class Storage:

    def __init__(self):
        self.data = {}

    def put(self, key, value, timestamp):
        if key not in self.data:
            self.data[key] = {}
        self.data[key][timestamp] = value


    def get(self, key):
        data = self.data
        if key == '':
            raise ValueError
        elif key != '*':
            data = {key: data.get(key, {})}


        result = {}
        for key, timestamp_data in data.items():
            result[key] = sorted(timestamp_data.items())

        return result

class DecoderError(ValueError):
    pass

class InterfaceError(Exception):
    pass


class Protocol(asyncio.Protocol):

    storage = Storage()

    def connection_made(self, transport):
        """Вызывается при установлении соединения.
        Аргумент - это транспорт, представляющий соединение канала.
        Чтобы получить данные, дождитесь вызовов data_received ()."""

        self.transport = transport


    def data_received(self, data):

        """Вызывается когда поступают данные
        Аргументы принемаются в байтах"""
        buffer = b''
        buffer += data
        try:
            decoded_data = buffer.decode('utf-8')
        except UnicodeDecodeError:
            return

        if not decoded_data.endswith('\n'):
            return
        try:
            if decoded_data.isspace():
                raise ValueError("wrong command")

        except ValueError as err:
            self.transport.write(f"error\n{err}\n\n".encode())
            return

        # отправляем на разбор
        try:
            commands = self.decode(decoded_data)
            if decoded_data == False:
                raise ValueError("wrong command")
        except ValueError as err:
            self.transport.write(f"error\n{err}\n\n".encode())
            return

        # выполняем команды и запоминаем результаты выполнения
        responses = []
        for command in commands:  # каждый tuple в списке отправляется в executor.run
            resp = self.interface_storage(*command)
            responses.append(resp)

        # собираем ответ для отправки
        response = self.encode(responses)

        # отправляем
        self.transport.write(response.encode())

    def interface_storage(self, method, *params):

        if method == 'put':
            response = self.storage.put(*params)
        elif method == 'get':
            response =  self.storage.get(*params)
        else:
            raise InterfaceError("Unsupported method")

        return response

    def valid(self, params):
        params = list(params)
        for lis in params:
            if params == '*':
                continue
            elif lis.isspace():
                raise ValueError
        return True



    def decode(self, data):
        parts = data.split("\n")

        commands = []

        for part in parts:
            if not part:
                continue
            try:
                method, params = part.split(" ", 1)
                if method == 'put':
                    key, value, timestamp = params.split()
                    commands.append((method, key, float(value), int(timestamp)))
                elif method == 'get':
                    if self.valid(params):
                        key = params
                        commands.append((method, key))
                    else:
                        raise ValueError("unknown params")
                else:
                    raise ValueError("unknown method")
            except ValueError:
                raise DecoderError("wrong command")

        return commands

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

        return (result + "\n")


def run_server(host, port):

    loop = asyncio.get_event_loop()
    coro = loop.create_server(Protocol, host, port)
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == '__main__':
    run_server('127.0.0.1', 8888)
