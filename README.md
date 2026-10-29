# PubSubX

PubSubX is a lightweight publish-subscribe messaging system implemented in Python. It provides a simple server and client for building distributed messaging applications.

**v0.2.0** — wire format changed: messages are now newline-delimited JSON with a read buffer that handles partial and multiple messages per read. This is not compatible with the 0.1.x framing.

## Installation

You can install PubSubX using pip:

```
pip install pubsubx
```

## Usage

### Starting the server

There are multiple ways to start the PubSubX server:

1. As a module:
   ```
   python -m pubsubx [--host HOST] [--port PORT]
   ```

2. Using the console script:
   ```
   pubsubx [--host HOST] [--port PORT]
   ```

3. In your Python code:
   ```python
   from pubsubx import PubSubServer

   server = PubSubServer(host='localhost', port=5000)
   server.start()
   ```

The default host is 'localhost' and the default port is 5000.

### Wire format

Messages are newline-delimited JSON: every message is a single JSON object terminated with `\n`. Both the server and client buffer incoming bytes so a single `recv` can contain a partial message or several complete messages.

### Using the client

Here's a simple example with two clients — one subscribes to a topic and the other publishes to it (the broker never echoes a publish back to its sender):

```python
from pubsubx import PubSubClient
import time

def on_message(topic, message):
    print(f"Received message on topic '{topic}': {message}")

# Subscriber
subscriber = PubSubClient()
subscriber.connect()
subscriber.on_message(on_message)
subscriber.subscribe("test_topic")

# Publisher
publisher = PubSubClient()
publisher.connect()

# Give the subscriber time to register before publishing
time.sleep(0.5)
publisher.publish("test_topic", "Hello, PubSubX!")
time.sleep(0.5)

publisher.disconnect()
subscriber.disconnect()
```

## License

This project is licensed under the MIT License.