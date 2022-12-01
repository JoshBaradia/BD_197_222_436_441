# COMPLETED
# keep pinging the brokers
# the first broker who pings the current node is the leader
import socket
from communication import *
import os
from threading import Thread
from time import time, sleep
leader_lastping = time() - 5

# Leader runs on port - 10000

LeaderPort = 9000
LeaderDied = False

# zookeeper is SERVER ONLY


def listen_to_brokers():
    global leader_lastping
    s = socket.socket()
    port = 9000
    s.bind(('0.0.0.0', port))
    s.listen(100)
    global LeaderDied
    while True:
        conn, addr = s.accept()
        req = ReceiveMessage(conn)
        # If leader didn't respond for specific amount of time
        # The first broker which pings zookeeper will be elected as leader
        if req["type"] == "ping": # ping
            if req["message"] == "ping":
                if req["sender"] != "leader":
                    if time() - leader_lastping > 2:
                        return_message = {"type": "ping"}
                        leader_lastping = time()
                    else:
                        return_message = {"type": "_ping_"}
                else:
                    return_message = {"type": "_ping_"}
                    leader_lastping = time()
        SendMessage(conn, return_message)

Thread(target = listen_to_brokers).start()

try:
    input("PRESS ENTER TO TERMINATE ZOOKEEPER:\n")
except:
    pass
pid = os.getpid()
os.kill(pid, 8)