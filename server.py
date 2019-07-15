import dataclasses
import re
import socket
import threading

NETWORK_INTERFACE = '127.0.0.1'
CONTROL_PORT = 21


@dataclasses.dataclass
class FTPSession:
    control: socket.socket
    buf: bytes = b''
    authenticated: bool = False
    username: str = None


def cmd_noop(session):
    session.control.sendall(b'200 Command okay\r\n')


def cmd_unknown(session):
    session.control.sendall(b'500 Not implemented\r\n')


def cmd_quit(session):
    # TODO: we shouldn't close if a data transfer is in progress
    session.username = None
    session.authenticated = False
    session.control.sendall(b"221 y'all come back now, ya hear?\r\n")
    session.control.close()


def cmd_user(): pass
def cmd_pass(): pass
def cmd_pwd(): pass
def cmd_cwd(): pass
def cmd_mkd(): pass
def cmd_list(): pass
def cmd_retr(): pass
def cmd_stor(): pass


COMMANDS = [
    (re.compile(r'^NOOP\r\n'), cmd_noop),
    (re.compile(r'^USER (\w+)\r\n'), cmd_user),
    (re.compile(r'^PASS (\w+)\r\n'), cmd_pass),
    (re.compile(r'^PWD\r\n'), cmd_pwd),
    (re.compile(r'^CWD ([\w/]+)\r\n'), cmd_cwd),
    (re.compile(r'^MKD ([\w/]+)\r\n'), cmd_mkd),
    (re.compile(r'^LIST ?([\w/]*)\r\n'), cmd_list),
    (re.compile(r'^RETR ([\w/]+)\r\n'), cmd_retr),
    (re.compile(r'^STOR ([\w/]+)\r\n'), cmd_stor),
    (re.compile(r'QUIT\r\n'), cmd_quit)
]


def dispatch(session, text):
    for pattern, func in COMMANDS:
        m = pattern.match(text)
        if m:
            args = m.groups() or []
            return func(session, *args)
    return cmd_unknown(session)


def listen_for_control_connections():
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as server_socket:
        server_socket.bind((NETWORK_INTERFACE, CONTROL_PORT))
        server_socket.listen()
        while True:
            yield server_socket.accept()


def _recv_line(s):
    while b'\r\n' not in s.buf:
        s.buf += s.control.recv(1024)
    index = s.buf.find(b'\r\n')
    line = s.buf[:index]
    s.buf = s.buf[index:]
    breakpoint()
    return line

def ftp_control_connection(control_socket, addr):
    # control connection uses telnet
    s = FTPSession(control_socket)
    s.control.sendall(b'220 Prepare yourself for Mongo FTP\r\n')

    try:
        while True:
            text = _recv_line(s).decode('ascii')
            dispatch(s, text)
    except OSError:
        # The connection was closed, we'll return so the thread can exit
        return


if __name__ == '__main__':
    for control_connection, addr in listen_for_control_connections():
        threading.Thread(target=ftp_control_connection, args=(control_connection, addr)).start()
