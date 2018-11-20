from kombu import Connection, Exchange, Queue, uuid

rpc_exchange = Exchange('rpc', 'direct', durable=True)
command_queue = Queue('command', exchange=rpc_exchange, routing_key='command')
reply_queue = Queue(name=uuid(), exclusive=True)

# connections

conn = Connection('amqp://guest:guest@localhost//', )
conn.connect()
# produce

corr_id = uuid()
producer = conn.Producer(serializer='json')
simple_queue = conn.SimpleQueue(name=reply_queue)
producer.publish(
    {'method': 'sum', 'params': [3, 4]},
    exchange=rpc_exchange,
    routing_key=command_queue.name,
    declare=[command_queue],
    reply_to=reply_queue.name,
    correlation_id=corr_id)

print('sent')
message = simple_queue.get(timeout=20)
print(message)
print(message.payload)
assert message.payload['result'] == 7
conn.close()
