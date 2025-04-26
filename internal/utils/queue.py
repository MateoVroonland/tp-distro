import pika

class ConsumerQueue:
    def __init__(self, connection, name, exchange_name):
        self.channel = connection.channel()
        
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='direct',
            durable=False
        )

        self.queue = self.channel.queue_declare(
            queue=name,
            durable=False,
            auto_delete=False,
            exclusive=False
        )
    
        self.channel.queue_bind(
            exchange=exchange_name,
            queue=name,
            routing_key=name
        )
        self.queue_name = name
        self.exchange_name = exchange_name

    def consume(self, auto_ack, callback):
        return self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )
    
    def publish(self, body):
        encoded_body = body.encode('utf-8')
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.queue_name,
            body=encoded_body
        )
    
    def set_qos(self, prefetch_count=1):
        self.channel.basic_qos(prefetch_count=prefetch_count)
    
    def start_consuming(self):
        self.channel.start_consuming()
        
    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        if self.channel.is_open:
            self.channel.close()

class ProducerQueue:
    def __init__(self, connection, name):
        self.channel = connection.channel()

        self.channel.exchange_declare(
            exchange=name,
            exchange_type='direct',
            durable=False
        )
        self.queue_name = name

    def publish(self, body):
        encoded_body = body.encode('utf-8')
        self.channel.basic_publish(
            exchange=self.queue_name,
            routing_key=self.queue_name,
            body=encoded_body
        )
    
    def close(self):
        if self.channel.is_open:
            self.channel.close()
