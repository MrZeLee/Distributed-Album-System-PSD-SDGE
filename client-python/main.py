import threading
import queue
import socket
import zmq
import sys

# Global queues for inter-thread communication
message_queue = queue.Queue()
tcp_send_queue = queue.Queue()
zmq_send_queue = queue.Queue()

# Shared flag indicating whether to use ZeroMQ for sending messages
use_zmq_for_sending = threading.Event()

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

def zmq_dealer_thread(zmq_port):
    """ ZeroMQ DEALER sends messages to the ROUTER. """
    context = zmq.Context()
    dealer = context.socket(zmq.DEALER)
    dealer.connect("tcp://localhost:"+str(zmq_port))  # Connect to the ROUTER

    while True:
        message = zmq_send_queue.get()
        if message == "exit":
            dealer.close()
            context.term()
            print("Exiting zmq_dealer_thread.")
            use_zmq_for_sending.clear()
            break
        message = message + "\n"
        dealer.send_string(message)

def zmq_router_thread(zmq_port):
    """ ZeroMQ ROUTER receives messages from DEALER(s) and forwards them to output_thread. """
    context = zmq.Context()
    router = context.socket(zmq.ROUTER)
    router.bind("tcp://*:" + str(zmq_port))  # Listen for DEALER connections

    while True:
        try:
            _, message = router.recv_multipart()
            if message.decode() == "exit":
                router.close()
                context.term()
                print("Exiting zmq_router_thread.")
                break
            message_queue.put(message.decode())
        except Exception as e:
            print(f"Failed to receive ZeroMQ message: {e}")
            break

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <port>")
        sys.exit(1)
    zmq_port = int(sys.argv[1])  # Get the port number from command line argument
    host = 'localhost'  # Server IP address
    port = 8000         # Server port
    zmq_url = "tcp://localhost:" + str(zmq_port)

    # Create a shared socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # Start all threads
    threads = [
        threading.Thread(target=output_thread),
        threading.Thread(target=input_thread),
        threading.Thread(target=tcp_send_thread, args=(sock, zmq_url,)),
        threading.Thread(target=tcp_receive_thread, args=(sock,)),
        threading.Thread(target=zmq_dealer_thread, args=(zmq_port,)),
        threading.Thread(target=zmq_router_thread, args=(zmq_port,))
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
        sock.close()

if __name__ == "__main__":
    main()

