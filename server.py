from kombu import Exchange, Queue, Connection
from kombu.mixins import ConsumerProducerMixin

rpc_exchange = Exchange('rpc', 'direct', durable=True)
command_queue = Queue('command', exchange=rpc_exchange, routing_key='command')


class Server(ConsumerProducerMixin):
    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=[command_queue],
            on_message=self.on_request,
            accept={'application/json'},
            prefetch_count=1,
        )]

    def on_request(self, message):
        print(message.properties)
        print(message.decode())
        if message.payload['method'] == 'sum':
            x, y = message.payload['params']

            self.producer.publish(
                {'result': x + y},
                exchange='',
                routing_key=message.properties['reply_to'],
                correlation_id=message.properties['correlation_id'],
                serializer='json')
            print('sent message')
            message.ack()
        else:
            raise RuntimeError('invalid message: %r' % message.decode())


# connections

conn = Connection('amqp://guest:guest@localhost//', heartbeat=10)
conn.connect()

server = Server(conn)
server.run()
