import contextlib
import dataclasses
import os
import re
import socket

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from .mongo import (
    authenticate,
    create_collection,
    get_file_or_document,
    list_collections,
    list_databases,
    list_documents,
    store_file_or_document
)

NETWORK_INTERFACE = '0.0.0.0'
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
    data_addr: tuple = None


###########
# HELPERS #
###########
def _get_working_directory_path(session):
    if session.current_db is None:
        return '/'
    if session.current_collection is None:
        return '/{}'.format(session.current_db)
    return '/{}/{}'.format(session.current_db, session.current_collection)

def _get_db_and_collection(path):
    components = path.split('/')
    current_db = components[1] or None
    try:
        current_collection = components[2]
    except IndexError:
        current_collection = None
    return current_db, current_collection

def _format_directories(dirs):
    return '\r\n'.join('drwxrwxr-x 1 0 0 4960 {}'.format(d) for d in dirs)

def _format_files(file_list):
    return '\r\n'.join('-rw-rw-r-- 1 0 0 {} {}'.format(int(f['value']), f['_id']) for f in file_list)

@contextlib.contextmanager
def data_connection(data_addr, control_socket):
    control_socket.sendall(b'150 Opening the data connection\r\n')

    data_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    data_socket.connect(data_addr)
    try:
        yield data_socket
    finally:
        control_socket.sendall(b'226 Closing data connection\r\n')
        data_socket.close()

def auth_required(func):
    def decorated(*args):
        session = args[0]
        if session.authenticated:
            return func(*args)
        else:
            session.control.sendall(b'530 ya gotta log in first\r\n')
    return decorated

################
# FTP COMMANDS #
################

def cmd_noop(session):
    session.control.sendall(b'200 Command okay\r\n')

def cmd_type(session, type_):
    # TODO: do we have to store this?
    message = '200 switching transfer mode to {}\r\n'.format(type_)
    session.control.sendall(message.encode('ascii'))

def cmd_syst(session):
    session.control.sendall(b'215 UNIX\r\n')

def cmd_unknown(session):
    session.control.sendall(b'502 Not implemented\r\n')

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
        message = '230 Authenticated as {}\r\n'.format(session.username)
        session.control.sendall(message.encode('ascii'))
    except OperationFailure:
        session.control.sendall(b'530 Invalid username or password\r\n')

def cmd_pwd(session):
    message = '257 "{}"\r\n'.format(_get_working_directory_path(session))
    session.control.sendall(message.encode('ascii'))

def cmd_port(session, host_string, port_string):
    data_host = '.'.join(host_string.split(','))
    data_port = int.from_bytes([int(n) for n in port_string.split(',')], 'big')
    session.data_addr = (data_host, data_port)
    session.control.sendall(b'200 Duely noted\r\n')

@auth_required
def cmd_list(session, path):
    # default to working directory when no path is supplied
    path = path or _get_working_directory_path(session)
    db, collection = _get_db_and_collection(path)

    if db is None:
        databases = list_databases(session.mongo_client)
        message = _format_directories(databases)
    elif collection is None:
        collections = list_collections(session.mongo_client, db)
        message = _format_directories(collections)
    else:
        documents = list_documents(session.mongo_client, db, collection)
        message = _format_files(documents)

    with data_connection(session.data_addr, session.control) as ds:
        ds.sendall(message.encode('ascii'))

@auth_required
def cmd_cwd(session, path):
    if os.path.isabs(path):
        result_path = path
    else:
        working_directory = _get_working_directory_path(session)
        result_path = os.path.normpath(os.path.join(working_directory, path))

    db, collection = _get_db_and_collection(result_path)
    session.current_db = db
    session.current_collection = collection
    session.control.sendall(b'250 Changing directory\r\n')

@auth_required
def cmd_retr(session, file_name):
    document = get_file_or_document(session.mongo_client, session.current_db, session.current_collection, file_name)
    with data_connection(session.data_addr, session.control) as ds:
        ds.sendall(document)

@auth_required
def cmd_mkd(session, path):
    if os.path.isabs(path):
        result_path = path
    else:
        working_directory = _get_working_directory_path(session)
        result_path = os.path.normpath(os.path.join(working_directory, path))

    db, collection = _get_db_and_collection(result_path)
    if collection is not None:
        create_collection(session.mongo_client, db, collection)
        message = '257 {} directory created\r\n'.format(result_path)
        session.control.sendall(message.encode('ascii'))
    else:
        session.control.sendall(b"550 Can't create top level directories create a nested directory\r\n")

@auth_required
def cmd_stor(session, file_name):
    with data_connection(session.data_addr, session.control) as ds:
        # read all the data that the client sends
        contents = ds.makefile().read()
        store_file_or_document(session.mongo_client, session.current_db, session.current_collection, file_name, contents)


###############
# DISPATCHING #
###############

COMMANDS = [
    (re.compile(r'^USER (\w+)\r\n'), cmd_user),
    (re.compile(r'^PASS (.+)\r\n'), cmd_pass),
    (re.compile(r'^TYPE ([IA])\r\n'), cmd_type),
    (re.compile(r'^PORT (\d+,\d+,\d+,\d+),(\d+,\d+)\r\n'), cmd_port),
    (re.compile(r'^PWD\r\n'), cmd_pwd),
    (re.compile(r'^LIST ?([\w/]*)\r\n'), cmd_list),
    (re.compile(r'^CWD ([\w/\.]+)\r\n'), cmd_cwd),
    (re.compile(r'^MKD ([\w/]+)\r\n'), cmd_mkd),
    (re.compile(r'^RETR ([\w/]+)\r\n'), cmd_retr),
    (re.compile(r'^STOR ([\w/\.]+)\r\n'), cmd_stor),
    (re.compile(r'^SYST\r\n'), cmd_syst),
    (re.compile(r'^NOOP\r\n'), cmd_noop),
    (re.compile(r'^QUIT\r\n'), cmd_quit)
]


def dispatch(session, text):
    print(text)
    for pattern, func in COMMANDS:
        m = pattern.match(text)
        if m:
            args = m.groups() or []
            return func(session, *args)
    return cmd_unknown(session)


##########
# SERVER #
##########

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
