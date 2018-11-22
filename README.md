Messaging example with a document channel and an rpc channel
==================================================

Rabbitmq server
----------------
    $ docker run --rm --name rabbitmq-broker -p 15672:15672 -p 5672:5672 rabbitmq:3-management

Running
-------

Run the following commands in their own shells

    $ python client.py
    
    $ python server.py
        
The client takes a json document as the trigger message. 
This is a list of tuples containing numbers to sum.

Example: 
```json
[
[3, 4],
[4, 5],
[1, 2]
]
``` 

Publish the document to the sum_batches queue in rabbit. 
Suggestion: Use the rabbitmq management UI at http://localhost:15672

Check the result by examining the sum_results queue

