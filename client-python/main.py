import socket
import zmq
import sys
import json
from rx import operators as ops
from rx.scheduler.threadpoolscheduler import ThreadPoolScheduler
from rx.subject.behaviorsubject import BehaviorSubject
from rx.subject.subject import Subject
import threading
import time

debug = True

# Reactive subjects for inter-thread communication
message_subject = Subject()
tcp_send_subject = Subject()
zmq_send_subject = Subject()
zmq_connect_subject = Subject()
terminate_subject = BehaviorSubject(False)
use_zmq_for_sending_subject = BehaviorSubject(False)

connected_pubs = set()
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
            effect = merge_localdata_images(effect)
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
                if "value" in localdata["images"][image_name]:
                    if "users" in localdata["images"][image_name]["value"]:
                        if user_name in localdata["images"][image_name]["value"]["users"]:
                            pass
                        else:
                            localdata["images"][image_name]["value"]["users"][user_name] = change["rating"]
                    else:
                        localdata["images"][image_name]["value"]["users"] = {user_name: change["rating"]}
                if debug: print(f"[DEBUG]User {user_name} rated image {image_name} with {change['rating']}")

    if debug: print("[DEBUG]Local data: ", localdata)
    process_pending_messages()

def merge_localdata_images(received_images):
    global localdata
    if debug: print("[DEBUG]Merging localdata images with received_images: ", received_images)
    # keep ratings from localdata
    for image, info in localdata.get('images', {}).items():
        if image in received_images:
            if 'value' in info and 'value' in received_images[image]:
                if 'users' in info['value'] and 'users' in received_images[image]['value']:
                    received_images[image]['value']['users'].update(info['value']['users'])
                else:
                    received_images[image]['value']['users'] = info['value'].get('users', {})
    if debug: print("[DEBUG]Merged images: ", received_images)
    return received_images

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
    time.sleep(5)
    zmq_send_subject.on_next(json.dumps(message))

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

def remove_versions_from_localdata(data):
    localdata_copy = data.copy()
    if debug: print("[DEBUG]Removing versions from localdata: ", localdata_copy)
    for v in localdata_copy.keys():
        if isinstance(localdata_copy[v], dict):
            for vv in localdata_copy[v].keys():
                if isinstance(localdata_copy[v][vv], dict):
                    if "value" in localdata_copy[v][vv].keys():
                        localdata_copy[v][vv] = localdata_copy[v][vv]["value"]
    if debug: print("[DEBUG]Removed versions from localdata: ", localdata_copy)
    return localdata_copy

def add_vector_clock(data):
    data_copy = data.copy()
    # add the vector clock to the data
    data_copy["clock"] = vector_clock.copy()
    return data_copy

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

def main():
    global username
    if len(sys.argv) < 2:
        print("Usage: python script.py <port>")
        sys.exit(1)
    zmq_port = int(sys.argv[1])
    host = 'localhost'
    port = 8000
    zmq_url = f"tcp://127.0.0.1:{zmq_port}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    context_pub = zmq.Context().instance()
    pub = context_pub.socket(zmq.PUB)
    pub.bind(zmq_url)
    context_sub = zmq.Context().instance()
    sub = context_sub.socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"")

    thread_pool_scheduler = ThreadPoolScheduler(4)

    def handle_output_thread():
        message_subject.pipe(
            ops.take_until(terminate_subject.pipe(ops.filter(lambda x: x))),
            ops.observe_on(thread_pool_scheduler)
        ).subscribe(
            on_next=lambda message: print(message, end=""),
            on_error=lambda e: print(f"Error1: {e}"),
            on_completed=lambda: print("Exiting output_thread.")
        )

    def handle_input_thread():
        def process_user_input(user_input):
            if use_zmq_for_sending_subject.value:
                if user_input == "/quit":
                    if debug: print("[DEBUG]json.dumps(remove_versions_from_localdata(localdata)): ", json.dumps(remove_versions_from_localdata(localdata)))
                    tcp_send_subject.on_next(f"/setMetadata {json.dumps(remove_versions_from_localdata(localdata))}")
                    tcp_send_subject.on_next("/quit")
                    zmq_send_subject.on_next("/disconnectFrom " + zmq_url)
                    zmq_connect_subject.on_next("/quit\n")
                    use_zmq_for_sending_subject.on_next(False)
                elif user_input == "/getImages":
                    images_info = get_images_info(localdata)
                    message_subject.on_next(json.dumps(images_info) + "\n")
                elif user_input == "/getMetadata":
                    message_subject.on_next(json.dumps(localdata) + "\n")
                elif user_input == "/getOrSet":
                    if users_or_set is not None:
                        message_subject.on_next(str(users_or_set.state()) + "\n")
                    if images_or_set is not None:
                        message_subject.on_next(str(images_or_set.state()) + "\n")
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
                        if localdata.get('images', {}).get(image_name, {}).get('value', {}).get('users', {}).get(username) is None:
                            broadcast_metadata_change(change)
                        else:
                            message_subject.on_next("You have already rated this image.\n")
                else:
                    zmq_send_subject.on_next(f"{username}: {user_input}")
            else:
                if user_input[-1] == "\n":
                    user_input = user_input[:-1]
                tcp_send_subject.on_next(user_input)

        while not terminate_subject.value:
            try:
                user_input = input()
                if terminate_subject.value:
                    break
                process_user_input(user_input)
            except EOFError:
                terminate_subject.on_next(True)
                message_subject.on_next("")
                tcp_send_subject.on_next("")
                zmq_send_subject.on_next("")
                zmq_connect_subject.on_next("")

    def edit_message(message):
        if message.startswith("/enterAlbum"):
            message_multipart = message.split(" ")
            if len(message_multipart) == 2:
                return f"{message_multipart[0]} {message_multipart[1]} {zmq_url}"
        return message

    def handle_tcp_send_thread(sock):
        tcp_send_subject.pipe(
            ops.take_until(terminate_subject.pipe(ops.filter(lambda x: x))),
            ops.observe_on(thread_pool_scheduler)
        ).subscribe(
            on_next=lambda message: sock.sendall(f"{edit_message(message)}\n".encode('utf-8')),
            on_error=lambda e: print(f"Error2: {e}"),
            on_completed=lambda: print("Exiting tcp_send_thread.")
        )

    def handle_tcp_receive_thread(sock):
        while not terminate_subject.value:
            try:
                data = sock.recv(1024)
                if data:
                    message = data.decode('utf-8').rstrip('\n')
                    if use_zmq_for_sending_subject.value:
                        if message.startswith("/connectTo "):
                            zmq_connect_subject.on_next(message)
                        if message.startswith("Metadata:"):
                            zmq_connect_subject.on_next(message)
                    else:
                        if message.startswith("/enterAlbum"):
                            print("You successfully entered the album.")
                            use_zmq_for_sending_subject.on_next(True)
                            tcp_send_subject.on_next("/getMetadata")
                        elif message.startswith("/loginOk "):
                            print("Logged in successfully.")
                            global username
                            username = message[9:]
                            global users_or_set, images_or_set
                            users_or_set = ORSet(username)
                            images_or_set = ORSet(username)
                        else:
                            if message[-1] != "\n":
                                message += "\n"
                            if debug: print(f"[DEBUG]Received message: {message}", end="")
                            message_subject.on_next(message)
                else:
                    print("Server disconnected.")
                    terminate_subject.on_next(True)
                    message_subject.on_next("")
                    tcp_send_subject.on_next("")
                    zmq_send_subject.on_next("")
                    zmq_connect_subject.on_next("")
            except Exception as e:
                terminate_subject.on_next(True)
                message_subject.on_next("")
                tcp_send_subject.on_next("")
                zmq_send_subject.on_next("")
                zmq_connect_subject.on_next("")

    def handle_zmq_pub_thread(context_pub, pub):
        zmq_send_subject.pipe(
            ops.take_until(terminate_subject.pipe(ops.filter(lambda x: x))),
            ops.observe_on(thread_pool_scheduler)
        ).subscribe(
            on_next=lambda message: pub.send_string(f"{message}\n"),
            on_error=lambda e: print(f"Error3: {e}"),
            on_completed=lambda: print("Exiting zmq_pub_thread.")
        )

    def handle_zmq_sub_thread(context_sub, sub):
        while not terminate_subject.value:
            try:
                message = sub.recv_string().rstrip('\n')
                if message:
                    if message.startswith("/disconnectFrom "):
                        if debug: print(f"[DEBUG]Disconnecting from {message[16:]}")
                        try:
                            sub.disconnect(message[16:])
                            connected_pubs.remove(message[16:])
                        except Exception as e:
                            if debug: print(f"[DEBUG]Failed to disconnect from {message[16:]}: {e}")
                    elif message.startswith("/getMetadataFromAll "):
                        if users_or_set is not None and images_or_set is not None:
                            if debug: print("[DEBUG]Sending metadata to all.")
                            zmq_send_subject.on_next("/sendMetadata " + message[20:] + " " + json.dumps(add_vector_clock({'users': users_or_set.state(), 'images': images_or_set.state()}))+"\n")
                    elif message.startswith("/sendMetadata "):
                        next_space = message[14:].find(" ")
                        _username = message[14:next_space + 14]
                        if _username == username:
                            data = json.loads(message[next_space + 15:])
                            initialize_vector_clock(data.get('clock', {}))
                            _localdata = remove_vector_clock(data)
                            if users_or_set is not None and images_or_set is not None:
                                received_users_state = _localdata.get('users', {})
                                received_images_state = _localdata.get('images', {})
                                temp_users_or_set = ORSet(username)
                                temp_images_or_set = ORSet(username)
                                temp_users_or_set.set_state(received_users_state)
                                temp_images_or_set.set_state(received_images_state)
                                users_or_set.merge(temp_users_or_set)
                                images_or_set.merge(temp_images_or_set)
                                localdata["users"] = users_or_set.elements()
                                localdata["images"] = images_or_set.elements()
                            else:
                                raise Exception("Users or images ORSet not initialized.")
                    elif message[0] == "{":
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
                        if message[-1] != "\n":
                            message += "\n"
                        message_subject.on_next(message)
            except Exception as e:
                print(f"Error4: {e}")
                terminate_subject.on_next(True)
                message_subject.on_next("")
                tcp_send_subject.on_next("")
                zmq_send_subject.on_next("")
                zmq_connect_subject.on_next("")

    def handle_zmq_connect(zmq_url, sub):
        zmq_connect_subject.pipe(
            ops.take_until(terminate_subject.pipe(ops.filter(lambda x: x))),
            ops.observe_on(thread_pool_scheduler)
        ).subscribe(
            on_next=lambda message: process_zmq_connect(message, zmq_url, sub),
            on_error=lambda e: print(f"Error5: {e}"),
            on_completed=lambda: print("Exiting zmq_connect.")
        )

    def process_zmq_connect(message, zmq_url, sub):
        global localdata, users_or_set, images_or_set
        if message.startswith("/quit"):
            for _pub in connected_pubs:
                try:
                    sub.disconnect(_pub)
                except Exception as e:
                    if debug: print(f"[DEBUG]Failed to disconnect from {_pub}: {e}")
            connected_pubs.clear()
        elif message.startswith("/connectTo "):
            if debug: print(f"[DEBUG]Connecting to {message[11:]}")
            try:
                sub.connect(message[11:])
                connected_pubs.add(message[11:])
            except Exception as e:
                if debug: print(f"[DEBUG]Failed to connect to {message[11:]}: {e}")
        elif message.startswith("Metadata:"):
            try:
                message_json = json.loads(message[9:])
                count = 0
                _pubs = extract_pubs(message_json)
                for _pub in _pubs:
                    if zmq_url != _pub:
                        if debug: print(f"[DEBUG]Connecting to {_pub}")
                        try:
                            sub.connect(_pub)
                            connected_pubs.add(_pub)
                            count += 1
                        except Exception as e:
                            if debug: print(f"[DEBUG]Failed to connect to {_pub}: {e}")
                if count == 0:
                    _localdata = remove_users_info(message_json)
                    if users_or_set is not None and images_or_set is not None:
                        for user, value in _localdata.get('users', {}).items():
                            users_or_set.add(user, value)
                        for image, value in _localdata.get('images', {}).items():
                            images_or_set.add(image, value)
                        localdata.update({"users": users_or_set.elements(), "images": images_or_set.elements()})
                else:
                    time.sleep(1)
                    zmq_send_subject.on_next(f"/getMetadataFromAll {username}")
            except Exception as e:
                print(f"Error6: {e}")
                terminate_subject.on_next(True)
                message_subject.on_next("")
                tcp_send_subject.on_next("")
                zmq_send_subject.on_next("")
                zmq_connect_subject.on_next("")

    threads = [
        threading.Thread(target=handle_output_thread),
        threading.Thread(target=handle_input_thread),
        threading.Thread(target=handle_tcp_send_thread, args=(sock,)),
        threading.Thread(target=handle_tcp_receive_thread, args=(sock,)),
        threading.Thread(target=handle_zmq_pub_thread, args=(context_pub, pub)),
        threading.Thread(target=handle_zmq_sub_thread, args=(context_sub, sub)),
        threading.Thread(target=handle_zmq_connect, args=(zmq_url, sub))
    ]

    for thread in threads:
        thread.start()

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt. Shutting down...")
        terminate_subject.on_next(True)
        message_subject.on_next("")
        tcp_send_subject.on_next("")
        zmq_send_subject.on_next("")
        zmq_connect_subject.on_next("")
    finally:
        pub.send_string(f"/disconnectFrom {zmq_url}\n")
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
