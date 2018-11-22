import socket

from kombu import Connection, Exchange, Queue, uuid, Consumer

rpc_exchange = Exchange('rpc', 'direct', durable=True)
command_queue = Queue('command', exchange=rpc_exchange, routing_key='command')
sum_batches = Queue('sum_batches', exchange=rpc_exchange, routing_key='sum_batches')
reply_queue = Queue(name=uuid(), exclusive=True)

conn = Connection('amqp://guest:guest@localhost//', heartbeat=10)
conn.connect()

corr_id = uuid()
producer = conn.Producer(serializer='json')
simple_queue = conn.SimpleQueue(name=reply_queue)


def on_request(message):
    for x, y in message.payload:
        producer.publish(
            {'method': 'sum', 'params': [x, y]},
            exchange=rpc_exchange,
            routing_key=command_queue.name,
            declare=[command_queue],
            reply_to=reply_queue.name,
            correlation_id=corr_id)
        print('sent')
        reply_message = simple_queue.get(timeout=2)
        print('got reply')
        reply_message.ack()
        print(reply_message.payload['result'])
    message.ack()


with Consumer(
        conn,
        queues=[sum_batches],
        on_message=on_request,
        accept={'application/json'},
        prefetch_count=1):
    while True:
        try:
            conn.drain_events(timeout=2)
        except socket.timeout:
            conn.heartbeat_check()
