import pika

class Queue:
    def __init__(self, connection, name, durable: bool = False, auto_delete: bool = False,
                 exclusive: bool = False):
        self.channel = connection.channel()
        self.queue = self.channel.queue_declare(
            queue=name, 
            durable=durable, 
            auto_delete=auto_delete, 
            exclusive=exclusive, 
        )
        self.queue_name = name

    def publish(self, body):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=body
        )

    def consume(self, auto_ack, callback):
        return self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )

    def close(self) -> None:
        if self.channel.is_open:
            self.channel.close()


def new_queue(connection, name: str, durable: bool = False, auto_delete: bool = False,
              exclusive: bool = False):
    return Queue(connection, name, durable, auto_delete, exclusive)
