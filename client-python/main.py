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
terminate = threading.Event()

def get_images_info(data):
    # get json data from localdata where data['images'] has a map of where the key is the image name and the value is another map where it has a key named
    # "users" that has another map where the key is the username and the value is the rating from 0 to 5
    # I want the name and mean rating of each image
    images = data.get('images', {})
    images_info = {}
    for image, info in images.items():
        if info is not None and 'users' in info:
            users = info['users']
            count = 0
            total = 0
            for _, rating in users.items():
                if rating is not None:
                    count += 1
                    total += int(rating)
            if count > 0:
                images_info[image] = "{:.2f}".format(total / count)
            else:
                images_info[image] = "0.00"
    return images_info

localdata = {}
username = ""

def output_thread():
    """ Thread that prints messages from other threads. """
    while True and not terminate.is_set():
        message = message_queue.get()
        if message == "exit":
            break
        print(message, end="")
    print("Exiting output_thread.")

def input_thread():
    """ Thread that captures terminal input and forwards it to queues. """
    while True and not terminate.is_set():
        try:
            user_input = input()
            if use_zmq_for_sending.is_set():
                if user_input == "/getImages":
                    images_info = get_images_info(localdata)
                    message_queue.put(json.dumps(images_info)+"\n")
                else:
                    zmq_send_queue.put(user_input)
            else:
                tcp_send_queue.put(user_input)
                if user_input == "exit":
                    break
        except EOFError:
            # Handle EOFError if input is redirected or piped
            tcp_send_queue.put("exit")
            break
    print("Exiting input_thread.")

def tcp_send_thread(sock, zmq_url):
    """ Thread that sends messages from a queue to the shared TCP socket. """
    while True and not terminate.is_set():
        message = tcp_send_queue.get()
        if message == "exit":
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
    print("Exiting tcp_send_thread.")

def tcp_receive_thread(sock):
    """ Thread that receives messages from the shared TCP socket and puts them in the output queue. """
    while True and not terminate.is_set():
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
                    elif "logged in " == message[0:10]:
                        global username
                        if message[-1] == "\n":
                            username = message[10:-1]
                        else:
                            username = message[10:]
                    message_queue.put(message)
            else:
                print("Server disconnected.")
                terminate.set()
                message_queue.put("exit")
                tcp_send_queue.put("exit")
                break
        except Exception as e:
            print(f"Failed to receive message: {e}")
            message_queue.put("exit")
            tcp_send_queue.put("exit")
            break
    print("Exiting tcp_receive_thread.")

def zmq_pub_thread(context_pub, pub):
    """ ZeroMQ DEALER sends messages to the ROUTER. """

    # While context is not terminated
    while context_pub:
        message = zmq_send_queue.get()
        if message == "exit":
            tcp_send_queue.put("/quit")
            use_zmq_for_sending.clear()
        elif "/getMetadata" == message:
            message_queue.put(json.dumps(localdata)+"\n")
        elif message[:20] == "/getMetadataFromAll ":
            time.sleep(2)
            pub.send_string(message)
        elif message[:27] == "/getMetadataFromAllRequest ":
            pub.send_string("/sendMetadata " + message[27:] + " " + json.dumps(localdata)+"\n")
        else:
            message = message + "\n"
            #send to all subscribers
            pub.send_string(message)
    print("Exiting zmq_pub_thread.")

def zmq_sub_thread(context_sub, sub):
    """ ZeroMQ ROUTER receives messages from DEALER(s) and forwards them to output_thread. """

    while context_sub:
        try:
            message = sub.recv_string()
            if message[0:3] == "tcp":
                if message[-1] == "\n":
                    message = message[:-1]
                sub.disconnect(message)
            elif message[:20] == "/getMetadataFromAll ":
                zmq_send_queue.put("/getMetadataFromAllRequest " + message[20:])
            elif message[:14] == "/sendMetadata ":
                # get the username
                next_space = message[14:].find(" ")
                _username = message[14:next_space+14]
                if _username == username:
                    data = message[next_space+15:]
                    if data[-1] == "\n":
                        data = data[:-1]
                    print("update localdata")
                    localdata.update(remove_users_info(json.loads(data)))
            elif message[0] == "{":
                if message[-1] == "\n":
                    message = message[:-1]
                localdata.update(remove_users_info(json.loads(message)))
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
                    localdata.update(remove_users_info(message_json))
                else:
                    zmq_send_queue.put("/getMetadataFromAll " + username)
            except Exception as e:
                print(message)
    print("Exiting zmq_connect.")

def extract_routers(data):
    routers = []
    users = data.get('users', {})
    for _, info in users.items():
        if info is not None and 'router' in info:
            routers.append(info['router'])
    return routers

def remove_users_info(data):
    # Remove all information from users but keep the keys
    for user in data["users"]:
        data["users"][user] = None
    return data

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

