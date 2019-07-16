import threading

from ftpmongo.server import (
    listen_for_control_connections,
    ftp_control_connection
)

if __name__ == '__main__':
    for control_connection, addr in listen_for_control_connections():
        threading.Thread(target=ftp_control_connection, args=(control_connection, addr)).start()
