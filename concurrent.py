# server.py 
# Fib microservice 

from socket import *
from fib import fib 
from threading import Threadd

def fib_server(address):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(address)
    sock.listen(5)

    while True:
        client,addr = sock.accept()
        print("connection", addr)
        Thread(target=fib_client, args=(client,))
