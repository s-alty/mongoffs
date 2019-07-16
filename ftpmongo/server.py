import dataclasses
import re
import socket

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from .mongo import (
    authenticate
)

NETWORK_INTERFACE = '127.0.0.1'
CONTROL_PORT = 21


@dataclasses.dataclass
class FTPSession:
    control: socket.socket
    buf: bytes = b''
    authenticated: bool = False
    username: str = None
    mongo_client: MongoClient = None
    current_db: str = None
    current_collection: str = None


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

def cmd_user(session, username):
    session.username = username
    session.authenticated = False
    session.control.sendall(b'331 Send the password\r\n')

def cmd_pass(session, password):
    try:
        session.mongo_client = authenticate(session.username, password)
        session.authenticated = True
        session.control.sendall(b'230 Authenticated as ' + session.username.encode('ascii') + b'\r\n')
    except OperationFailure:
        session.control.sendall(b'530 Invalid username or password\r\n')


# TODO: Write an authentication required decorator


def _get_working_directory_path(session):
    if session.current_db is None:
        return '/'
    if session.current_collection is None:
        return '/{}'.format(session.current_db)
    return '/{}/{}'.format(session.current_db, session.current_collection)


def cmd_pwd(session):
    message = '257 "{}"\r\n'.format(_get_working_directory_path(session))
    session.control.sendall(message.encode('ascii'))


def cmd_cwd(session): pass
def cmd_mkd(session): pass
def cmd_list(session): pass
def cmd_retr(session): pass
def cmd_stor(session): pass


COMMANDS = [
    (re.compile(r'^NOOP\r\n'), cmd_noop),
    (re.compile(r'^USER (\w+)\r\n'), cmd_user),
    (re.compile(r'^PASS (.+)\r\n'), cmd_pass),
    (re.compile(r'^PWD\r\n'), cmd_pwd),
    (re.compile(r'^CWD ([\w/]+)\r\n'), cmd_cwd),
    (re.compile(r'^MKD ([\w/]+)\r\n'), cmd_mkd),
    (re.compile(r'^LIST ?([\w/]*)\r\n'), cmd_list),
    (re.compile(r'^RETR ([\w/]+)\r\n'), cmd_retr),
    (re.compile(r'^STOR ([\w/]+)\r\n'), cmd_stor),
    (re.compile(r'^QUIT\r\n'), cmd_quit)
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
    index = s.buf.find(b'\r\n') + 2
    line = s.buf[:index]
    s.buf = s.buf[index:]
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
