import socket

from kombu import Exchange, Queue, Connection, Consumer, Producer


rpc_exchange = Exchange('rpc', 'direct', durable=True)
command_queue = Queue('command', exchange=rpc_exchange, routing_key='command')


class Server:
    def __init__(self, connection, producer):
        self.connection = connection
        self.producer = producer

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

    def run(self):
        with Consumer(
                self.connection,
                queues=[command_queue],
                on_message=self.on_request,
                accept={'application/json'},
                prefetch_count=1):
            while True:
                try:
                    self.connection.drain_events(timeout=2)
                except socket.timeout:
                    self.connection.heartbeat_check()


conn = Connection('amqp://guest:guest@localhost//', heartbeat=10)
conn.connect()

server = Server(conn, Producer(conn))
server.run()
