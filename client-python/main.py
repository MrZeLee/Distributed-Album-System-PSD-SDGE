import time
import threading
import queue
import socket
import zmq
import sys
import json

# Global queues for inter-thread communication
message_queue = queue.Queue()
tcp_send_queue = queue.Queue()
zmq_send_queue = queue.Queue()
zmq_connect_queue = queue.Queue()

# Shared flag indicating whether to use ZeroMQ for sending messages
use_zmq_for_sending = threading.Event()

localdata = {}

def output_thread():
    """ Thread that prints messages from other threads. """
    while True:
        message = message_queue.get()
        if message == "exit":
            print("Exiting output_thread.")
            break
        print(message, end="")

def input_thread():
    """ Thread that captures terminal input and forwards it to queues. """
    while True:
        try:
            user_input = input()
            if use_zmq_for_sending.is_set():
                zmq_send_queue.put(user_input)
            else:
                tcp_send_queue.put(user_input)
                if user_input == "exit":
                    break
        except EOFError:
            # Handle EOFError if input is redirected or piped
            tcp_send_queue.put("exit")
            break

def tcp_send_thread(sock, zmq_url):
    """ Thread that sends messages from a queue to the shared TCP socket. """
    while True:
        message = tcp_send_queue.get()
        if message == "exit":
            print("Exiting tcp_send_thread.")
            break
        elif "/enterAlbum " in message:
            message = message + " " + zmq_url
        try:
            message = message + "\n"
            sock.sendall(message.encode('utf-8'))
        except Exception as e:
            print(f"Failed to send message: {e}")
            message_queue.put("exit")
            break

def tcp_receive_thread(sock):
    """ Thread that receives messages from the shared TCP socket and puts them in the output queue. """
    while True:
        try:
            data = sock.recv(1024)
            if data:
                message = data.decode('utf-8')
                if use_zmq_for_sending.is_set():
                    zmq_connect_queue.put(message)
                else:
                    if "entered album" in message:
                        use_zmq_for_sending.set()
                        tcp_send_queue.put("/getMetadata")
                    message_queue.put(message)
            else:
                print("Server disconnected.")
                message_queue.put("exit")
                tcp_send_queue.put("exit")
                break
        except Exception as e:
            print(f"Failed to receive message: {e}")
            message_queue.put("exit")
            tcp_send_queue.put("exit")
            break

def zmq_pub_thread(context_pub, pub):
    """ ZeroMQ DEALER sends messages to the ROUTER. """

    # While context is not terminated
    while context_pub:
        message = zmq_send_queue.get()
        if message == "exit":
            use_zmq_for_sending.clear()
        elif "/getMetadata" == message:
            message_queue.put(json.dumps(localdata)+"\n")
        elif message == "/getMetadataFomAll":
            time.sleep(2)
            pub.send_string("/getMetadataFromAll")
        elif message == "/getMetadataFromAllRequest":
            pub.send_string(json.dumps(localdata)+"\n")
        else:
            message = message + "\n"
            #send to all subscribers
            pub.send_string(message)

def zmq_sub_thread(context_sub, sub):
    """ ZeroMQ ROUTER receives messages from DEALER(s) and forwards them to output_thread. """

    while context_sub:
        try:
            message = sub.recv_string()
            if message[0:3] == "tcp":
                if message[-1] == "\n":
                    message = message[:-1]
                sub.disconnect(message)
            elif message == "/getMetadataFromAll":
                zmq_send_queue.put("/getMetadataFromAllRequest")
            elif message[0] == "{":
                if message[-1] == "\n":
                    message = message[:-1]
                localdata.update(json.loads(message))
            else:
                message_queue.put(message)
            # message_queue.put(message.decode())
        except Exception as e:
            print(f"Failed to receive ZeroMQ message: {e}")
            break
    print("Exiting zmq_sub_thread.")

def zmq_connect(zmq_url,sub):
    while True:
        message = zmq_connect_queue.get()
        # check if \n is at the end of the message and remove it
        if message[-1] == "\n":
            message = message[:-1]
        if message == "exit":
            break
        elif message[0:3] == "tcp":
            # remove \n from the end of the message
            sub.connect(message)
        else:
            try:
                message_json = json.loads(message)
                count = 0
                routers = extract_routers(message_json)
                for router in routers:
                    if zmq_url != router:
                        count += 1
                        sub.connect(router)
                if count == 0:
                    localdata.update(message_json)
                else:
                    zmq_send_queue.put("/getMetadataFomAll")
            except Exception as e:
                print(message)

def extract_routers(data):
    routers = []
    users = data.get('users', {})
    for _, info in users.items():
        if info is not None and 'router' in info:
            routers.append(info['router'])
    return routers


def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <port>")
        sys.exit(1)
    zmq_port = int(sys.argv[1])  # Get the port number from command line argument
    host = 'localhost'  # Server IP address
    port = 8000         # Server port
    zmq_url = "tcp://127.0.0.1:" + str(zmq_port)

    # Create a shared socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    context_pub = zmq.Context().instance()
    pub = context_pub.socket(zmq.PUB)
    pub.bind("tcp://127.0.0.1:" + str(zmq_port))
    context_sub = zmq.Context().instance()
    sub = context_sub.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"")


    # Start all threads
    threads = [
        threading.Thread(target=output_thread),
        threading.Thread(target=input_thread),
        threading.Thread(target=tcp_send_thread, args=(sock, zmq_url,)),
        threading.Thread(target=tcp_receive_thread, args=(sock,)),
        threading.Thread(target=zmq_pub_thread, args=(context_pub,pub,)),
        threading.Thread(target=zmq_sub_thread, args=(context_sub,sub,)),
        threading.Thread(target=zmq_connect, args=(zmq_url,sub,))
    ]

    for thread in threads:
        thread.start()

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt. Shutting down...")
        tcp_send_queue.put("exit")
        zmq_send_queue.put("exit")
        message_queue.put("exit")
    finally:
        pub.send_string(zmq_url + "\n")
        try:
            pub.setsockopt(zmq.LINGER, 0)
            pub.close()
            sub.close()
            context_pub.term()
            context_sub.term()
            sock.close()
        except Exception as e:
            print(f"Failed to close socket: {e}")

if __name__ == "__main__":
    main()

