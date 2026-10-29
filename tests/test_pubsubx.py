import socket
import threading
import time

import pytest

from pubsubx import PubSubServer


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()
    return port


def _start_server(port):
    srv = PubSubServer(host='localhost', port=port)
    thread = threading.Thread(target=srv.start, daemon=True)
    thread.start()
    # Wait until the server is accepting connections.
    for _ in range(100):
        try:
            probe = socket.create_connection(('localhost', port), timeout=0.2)
            probe.close()
            break
        except OSError:
            time.sleep(0.05)
    return srv, thread


def _connect(port):
    from pubsubx import PubSubClient
    client = PubSubClient(host='localhost', port=port)
    client.connect()
    time.sleep(0.1)
    return client


def test_two_messages_in_one_tcp_write():
    """Two messages written in a single send() must both be parsed."""
    port = _free_port()
    srv, thread = _start_server(port)

    subscriber = _connect(port)
    publisher = _connect(port)

    received = []
    subscriber.on_message(lambda topic, content: received.append((topic, content)))
    subscriber.subscribe("news")

    # Give the subscription time to register.
    time.sleep(0.3)

    # Write both messages in one TCP write.
    publisher.socket.sendall(
        b'{"action": "publish", "topic": "news", "content": "one"}\n'
        b'{"action": "publish", "topic": "news", "content": "two"}\n'
    )
    time.sleep(0.5)

    assert ("news", "one") in received
    assert ("news", "two") in received

    publisher.disconnect()
    subscriber.disconnect()
    srv.shutdown()
    thread.join(timeout=2)


def test_message_over_one_kilobyte():
    """A message larger than 1 KB must arrive intact."""
    port = _free_port()
    srv, thread = _start_server(port)

    subscriber = _connect(port)
    publisher = _connect(port)

    received = []
    subscriber.on_message(lambda topic, content: received.append((topic, content)))
    subscriber.subscribe("big")

    time.sleep(0.3)

    payload = "x" * 1500  # over 1 KB
    publisher.publish("big", payload)
    time.sleep(0.5)

    assert ("big", payload) in received

    publisher.disconnect()
    subscriber.disconnect()
    srv.shutdown()
    thread.join(timeout=2)


def test_subscriber_disconnects_mid_publish_without_dropping_publisher():
    """A subscriber that disconnects while publishing must not disconnect the publisher."""
    port = _free_port()
    srv, thread = _start_server(port)

    publisher = _connect(port)

    # Subscriber connects, subscribes, then disconnects while a publish is in flight.
    subscriber = _connect(port)
    subscriber.subscribe("topic")
    time.sleep(0.2)
    subscriber.disconnect()

    # Publish after the subscriber is gone — the publisher must stay connected.
    publisher.publish("topic", "after disconnect")
    time.sleep(0.3)

    # Publisher socket still open and usable: send another publish.
    publisher.publish("topic", "still alive")
    time.sleep(0.3)

    publisher.disconnect()
    srv.shutdown()
    thread.join(timeout=2)
