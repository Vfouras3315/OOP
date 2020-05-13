import socket


class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def run_server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(socket.SOMAXCONN)

        self.process_data()

    def process_data(self):
        """Read get data"""

        self.conn, self.addr = self.sock.accept()
        while True:
            data = self.conn.recv(1024)

            print(data.decode('utf-8'))

    def close(self):
        self.conn.close()
