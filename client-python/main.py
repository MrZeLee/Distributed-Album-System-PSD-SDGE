import time
import threading
import queue
import socket
import zmq
import sys
import json

debug = True

# Global queues for inter-thread communication
message_queue = queue.Queue()
tcp_send_queue = queue.Queue()
zmq_send_queue = queue.Queue()
zmq_connect_queue = queue.Queue()

connected_pubs = set()

# Shared flag indicating whether to use ZeroMQ for sending messages
use_zmq_for_sending = threading.Event()
terminate = threading.Event()

vector_clock = {}
pending_messages = []
localdata = {}
username = ""

class ORSet:
    def __init__(self):
        self.m = {}
        self.c = 0

    def elements(self):
        dic = {}
        for e, value in self.m.items():
            dic[e] = value
        return dic 

    def contains(self, e):
        return e in self.m and len(self.m[e]['versions']) > 0

    def initialize(self, initial_data, replica_id):
        for e, value in initial_data.items():
            if debug: print(f"[DEBUG]Initializing ORSet with {e}: {value}")
            if isinstance(value, dict):
                if 'versions' in value.keys():
                    if e not in self.m:
                        self.m[e] = {'versions': value['versions'], 'value': value['value']}
                    else:
                        self.m[e] = {'versions': list(set(self.m[e]['versions'] + value['versions'])), 'value': value['value']}
                else:
                    self.c += 1
                    version = (replica_id, self.c)
                    if e not in self.m:
                        self.m[e] = {'versions': [], 'value': value}
                    self.m[e]['versions'].append(version)
            else:
                self.c += 1
                version = (replica_id, self.c)
                if e not in self.m:
                    self.m[e] = {'versions': [], 'value': value}
                self.m[e]['versions'].append(version)
        if debug: print(f"[DEBUG]Initialized ORSet with {self.m}")

    def add(self, e, value, replica_id):
        self.c += 1
        version = (replica_id, self.c)
        if e not in self.m:
            self.m[e] = {'versions': [], 'value': value}
        self.m[e]['versions'].append(version)
        try:
            self.m[e]['versions'] = list(set(self.m[e]['versions']))
        except:
            for i in range(len(self.m[e]['versions'])):
                self.m[e]['versions'][i] = frozenset(self.m[e]['versions'][i])
            self.m[e]['versions'] = list(set(self.m[e]['versions']))
            for i in range(len(self.m[e]['versions'])):
                self.m[e]['versions'][i] = list(self.m[e]['versions'][i])
        self.m[e]['value'] = value
        return (e, value, version, "add")

    def remove(self, e):
        if e in self.m:
            removed_versions = self.m[e]['versions']
            del self.m[e]
            return (e, removed_versions, "remove")
        return (e, [], "remove")

    def effect(self, operation):
        if debug: print(f"[DEBUG]Effecting operation: {operation}")
        try:
            if operation[2] == "remove":
                e = operation[0]
                removed_versions = operation[1]
                if e in self.m:
                    for version in removed_versions:
                        try:
                            if debug: print(f"[DEBUG]Removing version {version} from {self.m[e]['versions']}")
                            for v in self.m[e]['versions']:
                                if v[0] == version[0] and v[1] == version[1]:
                                    self.m[e]['versions'].remove(v)
                        except:
                            if debug: print(f"[DEBUG]Failed removing version {version} from {self.m[e]['versions']}")
                            pass
                    if len(self.m[e]['versions']) == 0:
                        del self.m[e]
            elif operation[3] == "add":
                e = operation[0]
                value = operation[1]
                version = operation[2]
                if e not in self.m:
                    self.m[e] = {'versions': [], 'value': value}
                self.m[e]['versions'].append(version)
                try:
                    self.m[e]['versions'] = list(set(self.m[e]['versions']))
                except:
                    for i in range(len(self.m[e]['versions'])):
                        self.m[e]['versions'][i] = frozenset(self.m[e]['versions'][i])
                    self.m[e]['versions'] = list(set(self.m[e]['versions']))
                    for i in range(len(self.m[e]['versions'])):
                        self.m[e]['versions'][i] = list(self.m[e]['versions'][i])
                self.m[e]['value'] = value
        except Exception as e:
            print(f"Error in effect: {e}")

users_or_set = ORSet()
images_or_set = ORSet()

def increment_vector_clock():
    global vector_clock, username
    if username not in vector_clock:
        vector_clock[username] = 0
    vector_clock[username] += 1

def update_vector_clock(received_clock):
    global vector_clock
    if debug: print("[DEBUG]Updating vector clock with received clock: ", received_clock)
    if debug: print("[DEBUG]Old vector clock: ", vector_clock)
    for node, clock in received_clock.items():
        if node not in vector_clock or clock > vector_clock[node]:
            vector_clock[node] = clock
    if debug: print("[DEBUG]New vector clock: ", vector_clock)

def is_causally_ready(received_clock, sender):
    global vector_clock
    if debug: print("[DEBUG]Checking if causally ready with received clock: ", received_clock)
    if debug: print("[DEBUG]Current vector clock: ", vector_clock)
    for node, clock in received_clock.items():
        if node not in vector_clock:
            if clock != 1:
                if debug: print("[DEBUG]Node not in vector clock, returning False.")
                return False
        elif sender == node and clock != vector_clock.get(node, 0) + 1 :
            if debug: print("[DEBUG]Clocks don't match, returning False.(sender==node)")
            return False
        elif sender != node and clock > vector_clock.get(node, 0):
            if debug: print("[DEBUG]Clocks don't match, returning False.(sender!=node)")
            if debug: print("[DEBUG]sender: ", sender, " node: ", node, " clock: ", clock, " vector_clock: ", vector_clock.get(node, 0))
            return False
    if debug: print("[DEBUG]Check causally returning True.")
    return True

def apply_metadata_change(change):
    if debug: print("[DEBUG]Applying change: ", change)
    global localdata, pending_messages
    action = change.get("action")
    user_name = change.get("user_name", "")
    image_name = change.get("image_name", "")
    effect = change.get("effect")

    if action == "add_user":
        users_or_set.effect(effect)
        localdata["users"] = users_or_set.elements()
        if debug: print(f"[DEBUG]Added user {user_name}")
    elif action == "remove_user":
        users_or_set.effect(effect)
        localdata["users"] = users_or_set.elements()
        if debug: print(f"[DEBUG]Removed user {user_name}")
    elif action == "add_image":
        images_or_set.effect(effect)
        localdata["images"] = images_or_set.elements()
        if debug: print(f"[DEBUG]Added image {image_name}")
    elif action == "remove_image":
        images_or_set.effect(effect)
        localdata["images"] = images_or_set.elements()
        if debug: print(f"[DEBUG]Removed image {image_name}")
    elif action == "rate_image":
        if image_name in localdata["images"]:
            if image_name not in localdata["ratings"]:
                localdata["ratings"][image_name] = {}
            localdata["ratings"][image_name][user_name] = change["rating"]
            if debug: print(f"[DEBUG]User {user_name} rated image {image_name} with {change['rating']}")

    if debug: print("[DEBUG]Local data: ", localdata)
    process_pending_messages()

# def apply_metadata_change(change):
#     if debug: print("[DEBUG]Applying change: ", change)
#     global localdata, pending_messages
#     change_version = change.get("version", 0)
#     action = change.get("action")
#     user_name = change.get("user_name", "")
#     image_name = change.get("image_name", "")
#
#     # Check for conflicting changes in pending messages
#     conflicts = [msg for msg in pending_messages if msg['change'].get('user_name', '') == user_name or msg['change'].get('image_name', '') == image_name]
#     if debug: print("[DEBUG]Conflicts: ", conflicts)
#
#     if change_version > localdata["version"]:
#         localdata["version"] = change_version
#         if action == "add_user":
#             if any(conflict['change']['action'] == "remove_user" for conflict in conflicts):
#                 # Resolve conflict
#                 resolved_change = resolve_conflict(conflicts[0]['change'], change)
#                 if resolved_change == change:
#                     localdata["users"][user_name] = None
#                     pending_messages.remove(conflicts[0])
#             else:
#                 if debug: print("[DEBUG]adding user: ", user_name)
#                 localdata["users"][user_name] = None
#
#         elif action == "remove_user":
#             if debug: print("[DEBUG]localdata[\"users\"]: ", localdata["users"])
#             if user_name in localdata["users"]:
#                 if any(conflict['change']['action'] == "add_user" for conflict in conflicts):
#                     # Resolve conflict
#                     resolved_change = resolve_conflict(conflicts[0]['change'], change)
#                     if resolved_change == change:
#                         del localdata["users"][user_name]
#                         pending_messages.remove(conflicts[0])
#                 else:
#                     if debug: print("[DEBUG]removing user: ", user_name)
#                     del localdata["users"][user_name]
#
#         elif action == "add_image":
#             if any(conflict['change']['action'] == "remove_image" for conflict in conflicts):
#                 # Resolve conflict
#                 resolved_change = resolve_conflict(conflicts[0]['change'], change)
#                 if resolved_change == change:
#                     localdata["images"][image_name] = {"hash": change["hash"], "size": change["size"], "users": {}}
#                     pending_messages.remove(conflicts[0])
#             else:
#                 localdata["images"][image_name] = {"hash": change["hash"], "size": change["size"], "users": {}}
#
#         elif action == "remove_image":
#             if image_name in localdata["images"]:
#                 if any(conflict['change']['action'] == "add_image" for conflict in conflicts):
#                     # Resolve conflict
#                     resolved_change = resolve_conflict(conflicts[0]['change'], change)
#                     if resolved_change == change:
#                         del localdata["images"][image_name]
#                         pending_messages.remove(conflicts[0])
#                 else:
#                     del localdata["images"][image_name]
#         # if action == "add_image":
#         #     image_name = change["image_name"]
#         #     localdata["images"][image_name] = {"hash": change["hash"], "size": change["size"], "users": {}}
#         # elif action == "remove_image":
#         #     image_name = change["image_name"]
#         #     if image_name in localdata["images"]:
#         #         del localdata["images"][image_name]
#         # elif action == "add_user":
#         #     user_name = change["user_name"]
#         #     localdata["users"][user_name] = None
#         # elif action == "remove_user":
#         #     user_name = change["user_name"]
#         #     if user_name in localdata["users"]:
#         #         del localdata["users"][user_name]
#         elif action == "rate_image":
#             image_name = change["image_name"]
#             user_name = change["user_name"]
#             rating = change["rating"]
#             if image_name in localdata["images"]:
#                 localdata["images"][image_name]["users"][user_name] = rating
#             else:
#                 if debug: print("[DEBUG]Image not found: ", image_name)
#     pending_messages = [msg for msg in pending_messages if msg['change']['user_name'] != user_name and msg['change']['image_name'] != image_name]
#     if debug: print("[DEBUG]Local data: ", localdata)
#     process_pending_messages()

def process_pending_messages():
    global pending_messages
    new_pending_messages = []
    for message in pending_messages:
        change = message["change"]
        sender = message["sender"]
        received_clock = message["clock"]
        if is_causally_ready(received_clock, sender):
            update_vector_clock(received_clock)
            apply_metadata_change(change)
        else:
            new_pending_messages.append(message)
    pending_messages = new_pending_messages

def resolve_conflict(change1, change2):
    # Example resolution strategy: prioritize based on vector clock comparison
    clock1, clock2 = change1['clock'], change2['clock']
    for user in clock1:
        if clock1[user] > clock2.get(user, 0):
            return change1
        elif clock1[user] < clock2.get(user, 0):
            return change2
    # If clocks are equal, resolve based on action type or other criteria
    # For simplicity, let's assume "remove_user" takes precedence over "add_user"
    if change1['action'] == "remove_user":
        return change1
    if change1['action'] == "remove_image":
        return change1
    return change2

def broadcast_metadata_change(change):
    global localdata, vector_clock, username
    increment_vector_clock()
    message = {
        "change": change,
        "clock": vector_clock.copy(),
        "sender": username
    }
    apply_metadata_change(change)
    process_pending_messages()
    time.sleep(10)
    zmq_send_queue.put(json.dumps(message))


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


def output_thread():
    """ Thread that prints messages from other threads. """
    while True and not terminate.is_set():
        message = message_queue.get()
        if message == "":
            continue
        print(message, end="")
    print("Exiting output_thread.")

def input_thread():
    """ Thread that captures terminal input and forwards it to queues. """
    while True:
        try:
            user_input = input()
            if terminate.is_set():
                break
            if use_zmq_for_sending.is_set():
                if user_input == "/quit":
                    tcp_send_queue.put("/quit")
                    zmq_send_queue.put("/quit")
                    zmq_connect_queue.put("/quit")
                    use_zmq_for_sending.clear()
                elif user_input == "/getImages":
                    images_info = get_images_info(localdata)
                    message_queue.put(json.dumps(images_info)+"\n")
                elif user_input == "/getMetadata":
                    message_queue.put(json.dumps(localdata)+"\n")
                elif user_input == "/getOrSet":
                    message_queue.put(str(users_or_set.m)+"\n")
                    message_queue.put(str(images_or_set.m)+"\n")
                elif user_input.startswith("/addImage "):
                    parts = user_input.split(" ")
                    if len(parts) == 4:
                        image_name, hash_val, size = parts[1], parts[2], parts[3]
                        change = {
                            "action": "add_image",
                            "image_name": image_name,
                            "hash": hash_val,
                            "size": size,
                            "effect": images_or_set.add(image_name, {"hash": hash_val, "size": size, "users": {}},username)
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/removeImage "):
                    parts = user_input.split(" ")
                    if len(parts) == 2:
                        image_name = parts[1]
                        change = {
                            "action": "remove_image",
                            "image_name": image_name,
                            "effect": images_or_set.remove(image_name)
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/addUser "):
                    parts = user_input.split(" ")
                    if len(parts) == 2:
                        user_name = parts[1]
                        change = {
                            "action": "add_user",
                            "user_name": user_name,
                            "effect": users_or_set.add(user_name, None, username)
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/removeUser "):
                    parts = user_input.split(" ")
                    if len(parts) == 2:
                        user_name = parts[1]
                        change = {
                            "action": "remove_user",
                            "user_name": user_name,
                            "effect": users_or_set.remove(user_name)
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/rateImage "):
                    parts = user_input.split(" ")
                    if len(parts) == 3:
                        image_name, rating = parts[1], parts[2]
                        change = {
                            "action": "rate_image",
                            "image_name": image_name,
                            "user_name": username,
                            "rating": rating
                        }
                        broadcast_metadata_change(change)
                else:
                    zmq_send_queue.put(user_input)
            else:
                if user_input[-1] == "\n":
                    user_input = user_input[:-1]
                tcp_send_queue.put(user_input)
        except EOFError:
            terminate.set()
            message_queue.put("")
            tcp_send_queue.put("")
            zmq_send_queue.put("")
            zmq_connect_queue.put("")

def tcp_send_thread(sock, zmq_url):
    """ Thread that sends messages from a queue to the shared TCP socket. """
    while True and not terminate.is_set():
        message = tcp_send_queue.get()
        if "/enterAlbum " == message[:12]:
            message = message + " " + zmq_url
        elif message == "":
            continue
        try:
            message = message + "\n"
            sock.sendall(message.encode('utf-8'))
        except Exception as e:
            terminate.set()
            message_queue.put("")
            zmq_send_queue.put("")
            zmq_connect_queue.put("")
    print("Exiting tcp_send_thread.")

def tcp_receive_thread(sock):
    """ Thread that receives messages from the shared TCP socket and puts them in the output queue. """
    while True and not terminate.is_set():
        try:
            data = sock.recv(1024)
            if data:
                message = data.decode('utf-8')
                if use_zmq_for_sending.is_set():
                    if message[-1] == "\n":
                        message = message[:-1]
                    if "/connectTo " == message[:11]:
                        zmq_connect_queue.put(message)
                    if "Metadata:" == message[:9]:
                        if debug: print("[DEBUG]Received metadata: ", message)
                        zmq_connect_queue.put(message)
                else:
                    if "/enterAlbum" == message[:11]:
                        print("You successfully entered the album.")
                        use_zmq_for_sending.set()
                        tcp_send_queue.put("/getMetadata")
                    elif "/loginOk " == message[:9]:
                        print("Logged in successfully.")
                        global username
                        if message[-1] == "\n":
                            username = message[9:-1]
                        else:
                            username = message[9:]
                    else:
                        message_queue.put(message)
            else:
                print("Server disconnected.")
                terminate.set()
                message_queue.put("")
                tcp_send_queue.put("")
                zmq_send_queue.put("")
                zmq_connect_queue.put("")
        except Exception as e:
            terminate.set()
            message_queue.put("")
            tcp_send_queue.put("")
            zmq_send_queue.put("")
            zmq_connect_queue.put("")
    print("Exiting tcp_receive_thread.")

def zmq_pub_thread(context_pub, pub, zmq_url):
    """ ZeroMQ DEALER sends messages to the ROUTER. """

    # While context is not terminated
    while context_pub and not terminate.is_set():
        message = zmq_send_queue.get()
        if debug: print("[DEBUG]zmq_pub_thread: ", message)
        if message == "":
            continue
        elif message[:20] == "/getMetadataFromAll ":
            time.sleep(2)
            pub.send_string(message)
        elif message[:27] == "/getMetadataFromAllRequest ":
            pub.send_string("/sendMetadata " + message[27:] + " " + json.dumps(add_vector_clock(localdata))+"\n")
        elif message == "/quit":
            if debug: print("[DEBUG]Telling subscribers to disconnect.")
            pub.send_string("/disconnectFrom "+ zmq_url + "\n")
        else:
            message = message + "\n"
            pub.send_string(message)
    print("Exiting zmq_pub_thread.")

def add_vector_clock(data):
    data_copy = data.copy()
    # add the vector clock to the data
    data_copy["clock"] = vector_clock.copy()
    return data_copy

def zmq_sub_thread(context_sub, sub):
    """ ZeroMQ ROUTER receives messages from DEALER(s) and forwards them to output_thread. """

    while context_sub and not terminate.is_set():
        try:
            message = sub.recv_string()
            if message == "":
                continue

            if message[-1] == "\n":
                if debug: print("[DEBUG]zmq_sub_thread: ", message[:-1])
            else:
                if debug: print("[DEBUG]zmq_sub_thread: ", message)

            if "/disconnectFrom " == message[:15]:
                if message[-1] == "\n":
                    message = message[:-1]
                if debug: print("[DEBUG]Disconnecting from ", message[15:])
                try:
                    sub.disconnect(message[15:])
                except Exception as _:
                    pass
                connected_pubs.remove(message[15:])
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
                    if debug: print("[DEBUG]localdata udpate with ", data)

                    message_json = json.loads(data)
                    initialize_vector_clock(message_json.get('clock', {}))
                    localdata.update(remove_vector_clock(remove_users_info(message_json)))
                    users_or_set.initialize(localdata.get('users', {}), username)
                    images_or_set.initialize(localdata.get('images', {}), username)
            elif message[0] == "{":
                if message[-1] == "\n":
                    message = message[:-1]
                message = json.loads(message)
                sender = message["sender"]
                received_clock = message["clock"]
                change = message["change"]
                if is_causally_ready(received_clock, sender):
                    update_vector_clock(received_clock)
                    apply_metadata_change(change)
                    process_pending_messages()
                else:
                    pending_messages.append(message)
            else:
                message_queue.put(message)
            # message_queue.put(message.decode())
        except Exception as e:
            terminate.set()
            message_queue.put("")
            tcp_send_queue.put("")
            zmq_send_queue.put("")
            zmq_connect_queue.put("")
    print("Exiting zmq_sub_thread.")

def initialize_vector_clock(received_clock):
    global vector_clock, username
    for node, clock in received_clock.items():
        vector_clock[node] = clock
    # Initialize own clock entry if not present
    if username not in vector_clock:
        vector_clock[username] = 0

def remove_vector_clock(data):
    # remove the vector clock from the data
    if "clock" in data:
        del data["clock"]
    return data

def zmq_connect(zmq_url,sub):
    global localdata, users_or_set, images_or_set
    while True and not terminate.is_set():
        message = zmq_connect_queue.get()
        # check if \n is at the end of the message and remove it
        if message == "":
            continue
        if message[-1] == "\n":
            message = message[:-1]
        if debug: print("[DEBUG]zmq_connect: ", message)
        if "/quit" == message:
            for _pub in connected_pubs:
                if debug: print("[DEBUG]Disconnecting from ", _pub)
                try:
                    sub.disconnect(_pub)
                except Exception as _:
                    pass
            connected_pubs.clear()
        elif "/connectTo " == message[:11]:
            if debug: print("[DEBUG]Connecting to ", message[11:])
            sub.connect(message[11:])
            connected_pubs.add(message[11:])
        elif "Metadata:" == message[:9]:
            if debug: print("[DEBUG]Received metadata: ", message)
            if message[-1] == "\n":
                message = message[:-1]
            try:
                message_json = json.loads(message[9:])
                count = 0
                _pubs = extract_pubs(message_json)
                for _pub in _pubs:
                    if zmq_url != _pub:
                        count += 1
                        if debug: print("[DEBUG]Connecting to ", _pub)
                        sub.connect(_pub)
                        connected_pubs.add(_pub)
                if count == 0:
                    if debug: print("[DEBUG]No other users to connect to, getting metadata central server.")
                    localdata.update(remove_users_info(message_json))
                    users_or_set.initialize(localdata.get('users', {}), username)
                    images_or_set.initialize(localdata.get('images', {}), username)
                    if debug: print("[DEBUG]Local data: ", localdata)
                    localdata.update({"users" : users_or_set.elements(), "images" : images_or_set.elements()})
                    if debug: print("[DEBUG]Local data after update: ", localdata)
                else:
                    if debug: print("[DEBUG]Getting metadata from other users.")
                    zmq_send_queue.put("/getMetadataFromAll " + username)
            except Exception as e:
                terminate.set()
                message_queue.put("")
                tcp_send_queue.put("")
                zmq_send_queue.put("")
                print(message)
            if debug: print("[DEBUG]Leave Metadata.")
        else:
            print(f"Unknown message: {message}")
    print("Exiting zmq_connect.")

def extract_pubs(data):
    routers = []
    users = data.get('users', {})
    for _, info in users.items():
        if info is not None and 'router' in info:
            routers.append(info['router'])
    return routers

def remove_users_info(data):
    # Remove all information from users but keep the keys
    for user in data["users"]:
        if isinstance(data["users"][user], dict) and "versions" in data["users"][user]:
            pass
        else:
            data["users"][user] = None
    return data

def add_version(data):
    data["version"] = 0
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
        threading.Thread(target=zmq_pub_thread, args=(context_pub,pub,zmq_url,)),
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
        terminate.set()
        message_queue.put("")
        tcp_send_queue.put("")
        zmq_send_queue.put("")
        zmq_connect_queue.put("")
    finally:
        pub.send_string("/disconnectFrom "+ zmq_url + "\n")
        try:
            pub.setsockopt(zmq.LINGER, 0)
            pub.close()
            sub.close()
            context_pub.term()
            context_sub.term()
            sock.close()
        except Exception as e:
            print(f"Failed to close socket: {e}")
        finally:
            print("Press Enter to exit.")

if __name__ == "__main__":
    main()

