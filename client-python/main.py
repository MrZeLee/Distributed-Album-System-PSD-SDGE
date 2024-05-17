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

class Dot:
    def __init__(self, replica_id, counter):
        self.replica_id = replica_id
        self.counter = counter

    def __hash__(self):
        return hash((self.replica_id, self.counter))

    def __eq__(self, other):
        return (self.replica_id, self.counter) == (other.replica_id, other.counter)

    def __repr__(self):
        return f"({self.replica_id}, {self.counter})"

    def to_json(self):
        return {
            'replica_id': self.replica_id,
            'counter': self.counter
        }
    
    @staticmethod
    def from_json(data):
        return Dot(data['replica_id'], data['counter'])


class ORSet:
    def __init__(self, replica_id):
        self.replica_id = replica_id
        self.counters = {replica_id: 0}
        self.add_set = {}  # Map from element to set of dots
        self.remove_set = {}  # Map from element to set of dots

    def _next_dot(self):
        counter = self.counters[self.replica_id] + 1
        self.counters[self.replica_id] = counter
        return Dot(self.replica_id, counter)

    def add(self, e, value):
        dot = self._next_dot()
        if e not in self.add_set:
            self.add_set[e] = {'versions': set(), 'value': value}
        self.add_set[e]['versions'].add(dot)
        return (e, value, dot.to_json(), "add")

    def remove(self, e):
        if e in self.add_set:
            removed_versions = self.add_set[e]['versions'].copy()
            if e not in self.remove_set:
                self.remove_set[e] = set()
            self.remove_set[e].update(removed_versions)
            del self.add_set[e]
            return (e, list([v.to_json() for v in removed_versions]), "remove")
        return (e, [], "remove")

    def elements(self):
        current_elements = {}
        for e, data in self.add_set.items():
            if e not in self.remove_set or not self.add_set[e]['versions'].issubset(self.remove_set[e]):
                current_elements[e] = {'versions': list([v.to_json() for v in data['versions']]), 'value': data['value']}
        return current_elements

    def contains(self, e):
        return e in self.add_set

    def merge(self, other):
        if debug: print("[DEBUG]Self: ", self.state())
        if debug: print("[DEBUG]Merging with: ", other.state())
        for e, data in other.add_set.items():
            if e not in self.add_set:
                self.add_set[e] = {'versions': set(), 'value': data['value']}
            self.add_set[e]['versions'].update(data['versions'])
        for e, dots in other.remove_set.items():
            if e not in self.remove_set:
                self.remove_set[e] = set()
            self.remove_set[e].update(dots)
            # if e in self.add_set:
            #     self.add_set[e]['versions'].difference_update(self.remove_set[e])
            #     if not self.add_set[e]['versions']:
            #         del self.add_set[e]
        for replica_id, counter in other.counters.items():
            if replica_id not in self.counters or counter > self.counters[replica_id]:
                self.counters[replica_id] = counter
        if debug: print("[DEBUG]Merged state: ", self.state())

    def state(self):
        if debug: print("[DEBUG]State remove_set: ", self.remove_set.items())
        return {
            'replica_id': self.replica_id,
            'counters': self.counters,
            'add_set': {e: {'versions': list([v.to_json() for v in data['versions']]), 'value': data['value']} for e, data in self.add_set.items()},
            'remove_set': {e: list([v.to_json() for v in dots]) for e, dots in self.remove_set.items()},
        }

    def set_state(self, state):
        if debug: print("[DEBUG]Setting state: ", state)
        self.replica_id = state['replica_id']
        self.counters = state['counters']
        self.add_set = {e: {'versions': set([ Dot.from_json(v) for v in data['versions']]), 'value': data['value']} for e, data in state['add_set'].items()}
        self.remove_set = {e: set([ Dot.from_json(v) for v in dots]) for e, dots in state['remove_set'].items()}
        if debug: print ("[DEBUG]Set state: ", self.state())

users_or_set = None
images_or_set = None

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

    if users_or_set is not None and images_or_set is not None:

        if action == "add_user":
            temp_users_or_set = ORSet(username)
            temp_users_or_set.set_state(effect)
            users_or_set.merge(temp_users_or_set)
            localdata["users"] = users_or_set.elements()

        elif action == "remove_user":
            temp_users_or_set = ORSet(username)
            temp_users_or_set.set_state(effect)
            users_or_set.merge(temp_users_or_set)
            localdata["users"] = users_or_set.elements()

        elif action == "add_image":
            temp_images_or_set = ORSet(username)
            temp_images_or_set.set_state(effect)
            images_or_set.merge(temp_images_or_set)
            localdata["images"] = images_or_set.elements()

        elif action == "remove_image":
            temp_images_or_set = ORSet(username)
            temp_images_or_set.set_state(effect)
            images_or_set.merge(temp_images_or_set)
            localdata["images"] = images_or_set.elements()

    elif action == "rate_image":
        if image_name in localdata["images"]:
            if image_name not in localdata["ratings"]:
                localdata["ratings"][image_name] = {}
            localdata["ratings"][image_name][user_name] = change["rating"]
            if debug: print(f"[DEBUG]User {user_name} rated image {image_name} with {change['rating']}")

    # if action == "add_user":
    #     users_or_set.effect(effect)
    #     localdata["users"] = users_or_set.elements()
    # elif action == "remove_user":
    #     users_or_set.effect(effect)
    #     localdata["users"] = users_or_set.elements()
    # elif action == "add_image":
    #     images_or_set.effect(effect)
    #     localdata["images"] = images_or_set.elements()
    # elif action == "remove_image":
    #     images_or_set.effect(effect)
    #     localdata["images"] = images_or_set.elements()
    # elif action == "rate_image":
    #     if image_name in localdata["images"]:
    #         if image_name not in localdata["ratings"]:
    #             localdata["ratings"][image_name] = {}
    #         localdata["ratings"][image_name][user_name] = change["rating"]
    #         if debug: print(f"[DEBUG]User {user_name} rated image {image_name} with {change['rating']}")

    if debug: print("[DEBUG]Local data: ", localdata)
    process_pending_messages()

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
    clock1, clock2 = change1['clock'], change2['clock']
    for user in clock1:
        if clock1[user] > clock2.get(user, 0):
            return change1
        elif clock1[user] < clock2.get(user, 0):
            return change2
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
    images = data.get('images', {})
    images_info = {}
    for image, info in images.items():
        info = info['value']
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
    while True and not terminate.is_set():
        message = message_queue.get()
        if message == "":
            continue
        print(message, end="")
    print("Exiting output_thread.")

def input_thread():
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
                    if users_or_set is not None:
                        message_queue.put(str(users_or_set.state())+"\n")
                    if images_or_set is not None:
                        message_queue.put(str(images_or_set.state())+"\n")
                elif user_input.startswith("/addImage "):
                    parts = user_input.split(" ")
                    if len(parts) == 4:
                        image_name, hash_val, size = parts[1], parts[2], parts[3]
                        images_or_set.add(image_name, {"hash": hash_val, "size": size, "users": {}})
                        change = {
                            "action": "add_image",
                            "effect": images_or_set.state()
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/removeImage "):
                    parts = user_input.split(" ")
                    if len(parts) == 2:
                        image_name = parts[1]
                        images_or_set.remove(image_name)
                        change = {
                            "action": "remove_image",
                            "effect": images_or_set.state()
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/addUser "):
                    parts = user_input.split(" ")
                    if len(parts) == 2:
                        user_name = parts[1]
                        users_or_set.add(user_name, None)
                        change = {
                            "action": "add_user",
                            "effect": users_or_set.state()
                        }
                        broadcast_metadata_change(change)
                elif user_input.startswith("/removeUser "):
                    parts = user_input.split(" ")
                    if len(parts) == 2:
                        user_name = parts[1]
                        users_or_set.remove(user_name)
                        change = {
                            "action": "remove_user",
                            "effect": users_or_set.state()
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
                        global users_or_set, images_or_set
                        users_or_set = ORSet(username)
                        images_or_set = ORSet(username)
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
    while context_pub and not terminate.is_set():
        message = zmq_send_queue.get()
        if debug: print("[DEBUG]zmq_pub_thread: ", message)
        if message == "":
            continue
        elif message[:20] == "/getMetadataFromAll ":
            time.sleep(2)
            pub.send_string(message)
        elif message[:27] == "/getMetadataFromAllRequest ":
            if users_or_set is not None and images_or_set is not None:
                pub.send_string("/sendMetadata " + message[27:] + " " + json.dumps(add_vector_clock({'users': users_or_set.state(), 'images': images_or_set.state()}))+"\n")
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
                next_space = message[14:].find(" ")
                _username = message[14:next_space+14]
                if _username == username:
                    data = message[next_space+15:]
                    if data[-1] == "\n":
                        data = data[:-1]
                    if debug: print("[DEBUG]localdata udpate with ", data)

                    if debug: print("[DEBUG]Recieved metadata: ", data)
                    message_json = json.loads(data)
                    if debug: print("[DEBUG]Recieved metadata json: ", message_json)
                    initialize_vector_clock(message_json.get('clock', {}))
                    _localdata = remove_vector_clock(message_json)
                    if debug: print("[DEBUG]_Local data: ", _localdata)
                    # localdata.update(remove_vector_clock(remove_users_info(message_json)))
                    if users_or_set is not None and images_or_set is not None:
                        if debug: print("[DEBUG]Users: ", _localdata.get('users', {}))
                        if debug: print("[DEBUG]Images: ", _localdata.get('images', {}))
                        received_users_state = _localdata.get('users', {})
                        received_images_state = _localdata.get('images', {})
                        
                        # Create temporary ORSets to merge states
                        temp_users_or_set = ORSet(username)
                        temp_images_or_set = ORSet(username)
                        
                        temp_users_or_set.set_state(received_users_state)
                        temp_images_or_set.set_state(received_images_state)
                        
                        users_or_set.merge(temp_users_or_set)
                        images_or_set.merge(temp_images_or_set)
                        
                        # Update localdata after merging
                        localdata["users"] = users_or_set.elements()
                        localdata["images"] = images_or_set.elements()
                    else:
                        raise Exception("Users or images ORSet not initialized.")
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
        except Exception as e:
            print(f"Error: {e}")
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
    if username not in vector_clock:
        vector_clock[username] = 0

def remove_vector_clock(data):
    if "clock" in data:
        del data["clock"]
    return data

def zmq_connect(zmq_url,sub):
    global localdata, users_or_set, images_or_set
    while True and not terminate.is_set():
        message = zmq_connect_queue.get()
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
                    _localdata = remove_users_info(message_json)
                    if users_or_set is not None and images_or_set is not None:
                        for user, value in _localdata.get('users', {}).items():
                            users_or_set.add(user, value)
                        for image, value in _localdata.get('images', {}).items():
                            images_or_set.add(image, value)
                        if debug: print("[DEBUG]Local data: ", localdata)
                        localdata.update({"users" : users_or_set.elements(), "images" : images_or_set.elements()})
                        if debug: print("[DEBUG]Local data after update: ", localdata)
                else:
                    if debug: print("[DEBUG]Getting metadata from other users.")
                    zmq_send_queue.put("/getMetadataFromAll " + username)
            except Exception as e:
                # print the error stack
                if debug: print("[DEBUG]Error1: ", e.with_traceback(None))
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
    global username
    if len(sys.argv) < 2:
        print("Usage: python script.py <port>")
        sys.exit(1)
    zmq_port = int(sys.argv[1])
    host = 'localhost'
    port = 8000
    zmq_url = "tcp://127.0.0.1:" + str(zmq_port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    context_pub = zmq.Context().instance()
    pub = context_pub.socket(zmq.PUB)
    pub.bind("tcp://127.0.0.1:" + str(zmq_port))
    context_sub = zmq.Context().instance()
    sub = context_sub.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"")

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

