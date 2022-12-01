from communication import *
import socket
from multiprocessing import Queue
from threading import Thread
import sys
from time import sleep

topic = sys.argv[1]

# queues for distributing data among multiple operations
queue_messages = Queue()

port = 8000 # default port for leader
# if connection gets crashed then the user will have to
# establishing connection with leader broker

def listen_for_messages(conn):
    while True:
        message = ReceiveMessage(conn)
        if message["type"] == "message":
            print(f"[{topic}] {message['content']}")


if "--from-beginning" in sys.argv:
    conn = socket.socket()
    conn.connect(("127.0.0.1", port))
    SendMessage(conn, {"sender": "data_from_beginning", "topic": topic})
    frombeginning = ReceiveMessage(conn)
    print(frombeginning["data"])
    
while True:
    try:
        conn = socket.socket()
        conn.connect(("127.0.0.1", port))
        connect_message = {
            "sender": "consumer",
            "type": "connect", 
            "message": "connect",
            "topic": topic
        }
        
        SendMessage(conn, connect_message)
        reply = ReceiveMessage(conn)
        listen_for_messages(conn)
    except Exception as err:
        print(err)
        print("RECONNECTING")
        sleep(2)

