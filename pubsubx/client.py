import socket
import threading
import json

class PubSubClient:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.receive_thread = None
        self.on_message_callback = None
        self.buffer = b''
        self._buffer_lock = threading.Lock()

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.running = True
        self.buffer = b''
        self.receive_thread = threading.Thread(target=self._receive_messages)
        self.receive_thread.start()
        print(f"Connected to server at {self.host}:{self.port}")

    def disconnect(self):
        self.running = False
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
        # Don't join the receive thread from within itself; the receive
        # thread is already exiting when it calls disconnect() (e.g. after
        # the server closes the connection).
        if self.receive_thread and threading.current_thread() is not self.receive_thread:
            self.receive_thread.join()
        print("Disconnected from server")

    def subscribe(self, topic):
        message = json.dumps({'action': 'subscribe', 'topic': topic})
        self._send_message(message)
        print(f"Subscribed to topic: {topic}")

    def publish(self, topic, content):
        message = json.dumps({'action': 'publish', 'topic': topic, 'content': content})
        self._send_message(message)
        print(f"Published message to topic: {topic}")

    def on_message(self, callback):
        self.on_message_callback = callback

    def _send_message(self, message):
        if self.socket:
            # Newline-delimited JSON framing: every message ends with '\n'.
            self.socket.sendall((message + '\n').encode('utf-8'))
        else:
            print("Not connected to server")

    def _receive_messages(self):
        self.socket.settimeout(1)  # Set a timeout for socket.recv()
        while self.running:
            try:
                data = self.socket.recv(4096)
            except socket.timeout:
                continue
            except (ConnectionError, OSError):
                break
            if not data:
                break
            # Newline-delimited JSON framing: buffer partial messages and
            # split on newlines so one read may contain several messages.
            with self._buffer_lock:
                self.buffer += data
                while b'\n' in self.buffer:
                    line, self.buffer = self.buffer.split(b'\n', 1)
                    if not line.strip():
                        continue
                    try:
                        message = json.loads(line.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        print("Invalid JSON received")
                        continue
                    try:
                        self._dispatch(message)
                    except Exception as e:
                        print(f"Error receiving message: {e}")

        self.disconnect()

    def _dispatch(self, message):
        if self.on_message_callback:
            self.on_message_callback(message.get('topic'), message.get('content'))
        else:
            print(f"Received message on topic '{message.get('topic')}': {message.get('content')}")