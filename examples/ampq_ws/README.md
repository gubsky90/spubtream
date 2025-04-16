Basic usage RabbitMQ and WebSocket with spubtream example 

- run rabbitmq.
- create queue ``ws.notifications``
- run ``go run .``
- connect to ws://127.0.0.1:9010 and send auth message ``{"token": "one,two"}``
- publish to ``ws.notifications`` message ``{"tags": ["one"], "data": 100500}``

