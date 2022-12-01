from communication import *
import socket
from threading import Thread
import sys
from time import sleep
conn = socket.socket()
topic = sys.argv[1]

init_message = {
    "sender": "producer",
    "type": "init",
    "topic": topic
}

init = socket.socket()
init.connect(("127.0.0.1", 8000))
SendMessage(init, init_message)

while True:
    msg = input(f"[{topic}] Enter Message: ")
    try:
        init = socket.socket()
        init.connect(("127.0.0.1", 8000))
        publish_data = {
            "sender": "producer",
            "type": "publish",
            "topic": topic,
            "content": msg
        }
        SendMessage(init, publish_data)
        init.close()
    except:
        sleep(2)
        print("RESENDING")
        init = socket.socket()
        init.connect(("127.0.0.1", 8000))
        publish_data = {
            "sender": "producer",
            "type": "publish",
            "topic": topic,
            "content": msg
        }
        SendMessage(init, publish_data)
        init.close()