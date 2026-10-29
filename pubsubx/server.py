import socket
import threading
import json

class PubSubServer:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients = {}       # client_socket -> set of topics
        self.topics = {}        # topic -> set of client_sockets
        self.lock = threading.RLock()
        self.running = True

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.server_socket.settimeout(1)  # Set a timeout for socket.accept()
        print(f"Server listening on {self.host}:{self.port}")
        try:
            while self.running:
                try:
                    client_socket, addr = self.server_socket.accept()
                    print(f"New connection from {addr}")
                    client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                    client_thread.start()
                except socket.timeout:
                    continue
                except OSError:
                    # The server socket was closed while shutting down; exit the
                    # accept loop cleanly instead of printing "Bad file descriptor".
                    if not self.running:
                        break
                    raise
        except KeyboardInterrupt:
            print("\nShutting down the server...")
        finally:
            self.shutdown()

    def handle_client(self, client_socket):
        with self.lock:
            self.clients[client_socket] = set()
        buffer = b''
        try:
            while self.running:
                try:
                    data = client_socket.recv(4096)
                except (ConnectionError, OSError):
                    break
                if not data:
                    break
                # Newline-delimited JSON framing: a read may contain a partial
                # message or several messages, so buffer and split on newlines.
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if not line.strip():
                        continue
                    try:
                        message = json.loads(line.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        print("Invalid JSON received")
                        continue
                    try:
                        action = message.get('action')
                        if action == 'subscribe':
                            self.subscribe(client_socket, message['topic'])
                        elif action == 'publish':
                            self.publish(client_socket, message['topic'], message['content'])
                    except Exception as e:
                        print(f"Error handling message: {e}")
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            self.disconnect_client(client_socket)

    def subscribe(self, client_socket, topic):
        with self.lock:
            self.topics.setdefault(topic, set()).add(client_socket)
            self.clients.setdefault(client_socket, set()).add(topic)
        print(f"Client subscribed to topic: {topic}")

    def publish(self, sender_socket, topic, content):
        # Iterate over a snapshot so a disconnecting subscriber never mutates
        # the set we are iterating. Sends happen outside the lock.
        with self.lock:
            recipients = list(self.topics.get(topic, set()))
        message = (json.dumps({'topic': topic, 'content': content}) + '\n').encode('utf-8')
        dead = []
        for client in recipients:
            if client == sender_socket:  # Don't send the message back to the sender
                continue
            try:
                client.sendall(message)
            except Exception as e:
                print(f"Error sending message to client: {e}")
                dead.append(client)
        # Remove dead clients after iteration is complete.
        with self.lock:
            for client in dead:
                self.disconnect_client_locked(client)
        print(f"Message published to topic: {topic}")

    def disconnect_client(self, client_socket):
        with self.lock:
            self.disconnect_client_locked(client_socket)

    def disconnect_client_locked(self, client_socket):
        # Caller must hold self.lock. Idempotent: safe to call for a socket
        # that has already been removed.
        if client_socket not in self.clients:
            return
        for topic in list(self.clients[client_socket]):
            self.topics.get(topic, set()).discard(client_socket)
            if not self.topics.get(topic):
                self.topics.pop(topic, None)
        self.clients.pop(client_socket, None)
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            client_socket.close()
        except Exception:
            pass
        print("Client disconnected")

    def shutdown(self):
        self.running = False
        with self.lock:
            for client_socket in list(self.clients.keys()):
                self.disconnect_client_locked(client_socket)
        try:
            self.server_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.server_socket.close()
        print("Server shut down")

def run_server():
    server = PubSubServer()
    server.start()

if __name__ == "__main__":
    run_server()