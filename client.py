import socket

from kombu import Connection, Exchange, Queue, uuid, Consumer

import rpc_channel

sums_exchange = Exchange('sums', 'direct', durable=True)
sum_batches = Queue('sum_batches', exchange=sums_exchange, routing_key='sum_batches')
reply_queue = Queue(name=uuid(), exclusive=True)
sum_results = Queue('sum_results', exchange=sums_exchange, routing_key='sum_results')

conn = Connection('amqp://guest:guest@localhost//', heartbeat=10)
conn.connect()

corr_id = uuid()
producer = conn.Producer(serializer='json')
simple_queue = conn.SimpleQueue(name=reply_queue)


def on_request(message):
    for x, y in message.payload:
        producer.publish(
            {'method': 'sum', 'params': [x, y]},
            exchange=rpc_channel.rpc_exchange,
            routing_key=rpc_channel.command_queue.name,
            declare=[rpc_channel.command_queue],
            reply_to=reply_queue.name,
            correlation_id=corr_id)
        print('sent')
        reply_message = simple_queue.get(timeout=2)
        print('got reply')
        producer.publish(
            {'x': x, 'y': y, 'sum': reply_message.payload['result']},
            exchange=sums_exchange,
            routing_key=sum_results.routing_key,
            declare=[sum_results])
        reply_message.ack()
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
