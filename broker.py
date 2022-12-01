from communication import *
import socket
from communication import *
import sys
import os
from queue import Queue
from threading import Thread
from time import sleep
import random
from time import time
# using queues to distribute messages among consumers
queues = {}
# queues["topic_name"] gives the queue related to topic
# all active topics (from producer and/or consumer side) will have a queue. 
# when a producer publishes a new message then 



base_dir = os.path.join(os.getcwd(), "broker")

def write_to_log(message):
    log_file = open(os.path.join(base_dir, "log.txt"), "a")
    log_file.write(f"{json.dumps(message)}\n")
    log_file.close()

def CheckForNewTopicFS(topic):
    if f"{topic}_p1.txt" not in os.listdir(base_dir):
        open(os.path.join(base_dir, f"{topic}_p0.txt"), "a").close()
        open(os.path.join(base_dir, f"{topic}_p1.txt"), "a").close()
        open(os.path.join(base_dir, f"{topic}_p2.txt"), "a").close()

def fetch_topic_data(topic):
    f1 = open(os.path.join(base_dir, f"{topic}_p0.txt"), "r").read()
    f2 = open(os.path.join(base_dir, f"{topic}_p1.txt"), "r").read()
    f3 = open(os.path.join(base_dir, f"{topic}_p2.txt"), "r").read()
    return f1 + f2 + f3

Leader = False

def send_heart_beats():
    global Leader                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
    while True:
        init = socket.socket()
        init.connect(("127.0.0.1", 9000))
        init_message = {
            "type": "ping",
            "message": "ping",
            "sender": "leader" if Leader else "standby",
        }
        SendMessage(init, init_message)
        init_response = ReceiveMessage(init)
        # 
        if init_response["type"] == "ping":
            Thread(target = start_broker, daemon = True).start()
            Leader = True
        else:
            pass
        init.close()
        sleep(1)

def connect_to_consumer(consumer: socket.socket, topic):
    global queues
    print("CONNECTED TO CONSUMER")
    if topic not in queues:
        queues[topic] = []
    MyQueue = Queue()
    queues[topic].append(MyQueue)
    while True:
        message = MyQueue.get()
        message_data = {
            "sender": "broker",
            "type": "message",
            "topic": topic,
            "content": message
        }
        SendMessage(consumer, message_data)
    consumer.close()


def publish_message(message):
    if message["topic"] in queues:
        if message["content"] != None: 
            for num in range(len(queues[message["topic"]])):
                queues[message["topic"]][num].put(message["content"])
    else:
        message["topic"] = []
    # writing message to random partition
    partition_index = random.randrange(3)
    filepath = os.path.join(base_dir, f"{message['topic']}_p{partition_index}.txt")
    writefile = open(filepath, "a")
    writefile.write(f"{message['content']}\n")
    write_to_log({"type": "message", "time": time(), "content": message["content"], "topic": message["topic"]})

def start_broker():
    print("BROKER STARTED")
    brkr = socket.socket()
    brkr.bind(("", 8000))
    brkr.listen(100)
    while True:
        conn, addr = brkr.accept()
        message = ReceiveMessage(conn)
        # consumer will have socket connection
        # producer will have single connection
        CheckForNewTopicFS(message["topic"])
        if message["sender"] == "producer":
            if message["type"] == "publish":
                publish_message(message)
            elif message["type"] == "init":
                CheckForNewTopicFS(message["topic"])
        elif message["sender"] == "consumer":
            CheckForNewTopicFS(message["topic"])
            SendMessage(conn, {"status": "done"})
            Thread(target = connect_to_consumer, args = [conn, message["topic"]]).start()
        elif message["sender"] == "data_from_beginning":
            print(f"DATA FROM BEGINNING - TOPIC {message['topic']}")
            senddata = fetch_topic_data(message["topic"])
            SendMessage(conn, {"type": "data_from_beginning", "topic": message["topic"], "data": senddata})
Thread(target = send_heart_beats).start()

try:
    input("PRESS ENTER TO TERMINATE BROKER:")
except:
    pass
pid = os.getpid()
os.kill(pid, 8)