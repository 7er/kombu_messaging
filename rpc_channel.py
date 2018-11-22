from kombu import Exchange, Queue

rpc_exchange = Exchange('rpc', 'direct', durable=True)
command_queue = Queue('command', exchange=rpc_exchange, routing_key='command')
