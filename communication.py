import socket
import json
from textwrap import wrap

def SendMessage(connection: socket.socket, message: dict):
    msg = json.dumps(message)
    send_list = wrap(msg, 1024)
    connection.send(json.dumps({"type": "message_metadata", "message_length": len(send_list), "message_size": len(message)}).encode("utf-8"))
    connection.recv(1024)
    for item in send_list:
        connection.send(item.encode("utf-8"))

def ReceiveMessage(connection: socket.socket):
    metadata = connection.recv(1024)
    # print(metadata)
    message_config = json.loads(metadata.decode("utf-8"))
    connection.send("OK".encode("utf-8"))
    message_data = []
    for _ in range(message_config["message_length"]):
        message_data.append(connection.recv(1024))
    message = json.loads(b"".join(message_data).decode("utf-8"))
    if len(message) != message_config["message_size"]:
        message["transfer_status"] = "ERROR"
    else:
        message["transfer_status"] = "DONE"
    return message