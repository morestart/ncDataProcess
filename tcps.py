import os
from socketserver import BaseRequestHandler, TCPServer
import json
import sqlite3
import time


class EchoHandler(BaseRequestHandler):
    def handle(self):
        print('Got connection from', self.client_address)
        while True:
            msg = self.request.recv(1024)
            data = msg.decode('utf-8')
            data = json.loads(data)

            sql_msg = "INSERT INTO SENSOR (X,Y,Z,lat,lon,speed, time) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, '{6}')"\
                .format(data.get('x'), data.get('y'), data.get('z'), data.get('lat'), data.get('lon'), data.get('speed'), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            print(sql_msg)
            c.execute(sql_msg)
            conn.commit()
            # conn.close()


def creat_table():
    c.execute("""CREATE TABLE SENSOR (
                    X DOUBLE NOT NULL,
                    Y DOUBLE NOT NULL,
                    Z DOUBLE NOT NULL,
                    lat DOUBLE NOT NULL,
                    lon DOUBLE NOT NULL,
                    speed DOUBLE NOT NULL,
                    time TEXT)
                    """)
    print("Table creat successfully")
    conn.commit()
    # conn.close()


if __name__ == '__main__':
    print("waiting for connection..")

    if not os.path.exists('sensor.db'):
        conn = sqlite3.connect('sensor.db')
        c = conn.cursor()
        creat_table()
    else:
        conn = sqlite3.connect('sensor.db')
        c = conn.cursor()

    serv = TCPServer(('', 8000), EchoHandler)
    serv.serve_forever()
