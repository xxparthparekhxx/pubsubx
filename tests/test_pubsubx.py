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


def test_server_shutdown_no_client_thread_exception():
    """Server shutdown must not raise RuntimeError in client receive threads.

    When the server closes a connection, the client's receive thread calls
    disconnect() itself; disconnect() must not try to join the running
    thread (which would raise RuntimeError: cannot join current thread).
    Exceptions in client threads are captured via threading.excepthook.
    """
    port = _free_port()
    srv, thread = _start_server(port)

    caught = []
    old_hook = threading.excepthook

    def hook(args):
        caught.append(args.exc_value)

    threading.excepthook = hook
    try:
        subscriber = _connect(port)
        publisher = _connect(port)
        subscriber.subscribe("topic")
        time.sleep(0.2)

        # User-facing disconnect (joins the receive thread from outside).
        subscriber.disconnect()
        time.sleep(0.2)

        # Server shutdown closes the publisher's socket; its receive thread
        # breaks out and calls disconnect() from within itself.
        srv.shutdown()
        thread.join(timeout=2)
        # Give the publisher's receive thread time to exit cleanly.
        time.sleep(0.5)
    finally:
        threading.excepthook = old_hook

    publisher.disconnect()
    assert not caught, f"exceptions in client threads: {caught}"


def test_client_receive_thread_exits_on_server_shutdown():
    """When the server stops, the client's receive thread must exit promptly."""
    port = _free_port()
    srv, thread = _start_server(port)

    client = _connect(port)
    client.subscribe("topic")
    time.sleep(0.2)

    srv.shutdown()
    thread.join(timeout=2)

    # The client's receive thread should finish within ~2 s once the server is gone.
    client.receive_thread.join(timeout=2)
    assert not client.receive_thread.is_alive(), "client receive thread did not exit after server shutdown"

    client.disconnect()


class _AcceptErrorInjector:
    """Wraps a listening socket, delegating everything but accept(), which
    raises ConnectionAbortedError on the first call and then delegates."""

    def __init__(self, sock):
        self._sock = sock
        self.accept_calls = 0

    def accept(self):
        self.accept_calls += 1
        if self.accept_calls == 1:
            raise ConnectionAbortedError("Software caused connection abort")
        return self._sock.accept()

    def __getattr__(self, name):
        return getattr(self._sock, name)


def test_transient_accept_error_does_not_kill_listener():
    """A transient accept() error must not kill the listener.

    The listening socket is wrapped so its very first accept() raises
    ConnectionAbortedError while the server is running. The accept loop must
    log it and keep accepting, so a client can still connect and receive a
    published message afterwards.
    """
    port = _free_port()
    srv = PubSubServer(host='localhost', port=port)
    srv.server_socket = _AcceptErrorInjector(srv.server_socket)
    thread = threading.Thread(target=srv.start, daemon=True)
    thread.start()

    # Wait until the server is accepting connections (this also proves the
    # loop survived the first accept() raising).
    for _ in range(200):
        try:
            probe = socket.create_connection(('localhost', port), timeout=0.2)
            probe.close()
            break
        except OSError:
            time.sleep(0.05)
    assert srv.server_socket.accept_calls >= 1, "accept loop never raised the injected error"

    received = []
    subscriber = _connect(port)
    subscriber.socket.settimeout(5.0)
    subscriber.on_message(lambda topic, content: received.append((topic, content)))
    subscriber.subscribe("news")
    time.sleep(0.3)

    publisher = _connect(port)
    publisher.socket.settimeout(5.0)
    time.sleep(0.3)
    publisher.publish("news", "hello after transient accept error")
    time.sleep(0.5)

    assert ("news", "hello after transient accept error") in received

    publisher.disconnect()
    subscriber.disconnect()
    srv.shutdown()
    thread.join(timeout=2)


def test_shutdown_is_idempotent(capsys):
    """shutdown() must be safe to call twice and print "Server shut down" once."""
    port = _free_port()
    srv, thread = _start_server(port)

    srv.shutdown()
    srv.shutdown()  # Second call must be a no-op, not raise.

    thread.join(timeout=2)

    out = capsys.readouterr().out
    assert out.count("Server shut down") == 1, f"expected exactly one shutdown message, got: {out!r}"
