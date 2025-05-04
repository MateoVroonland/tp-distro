import hashlib
import logging
import os
from dotenv import load_dotenv
import pika
import signal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def hash_string(s: str, n: int) -> int:
    """Hash a string to an integer between 1 and n (inclusive)"""
    h = hashlib.new('md5')
    h.update(s.encode('utf-8'))
    hash_value = int.from_bytes(h.digest()[:4], byteorder='big')
    return (hash_value % n) + 1

class Message:
    def __init__(self, body, client_id, delivery_tag, channel):
        self.body = body
        self.client_id = client_id
        self._delivery_tag = delivery_tag
        self._channel = channel

    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery_tag, multiple=False)

    def nack(self, requeue=False):
        self._channel.basic_nack(delivery_tag=self._delivery_tag, multiple=False, requeue=requeue)

class ConsumerQueue:
    def __init__(self, connection, name, exchange_name):
        self.channel = connection.channel()
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type='direct',
            durable=False
        )

        app_id = os.environ.get('ID', '1')
        self.queue_name = f"{name}_{app_id}"

        self.queue = self.channel.queue_declare(
            queue=self.queue_name,
            durable=False,
            auto_delete=False,
            exclusive=False
        )
    
        self.channel.queue_bind(
            exchange=exchange_name,
            queue=self.queue_name,
            routing_key=app_id
        )
        self.exchange_name = exchange_name

    def message_from_delivery(self, ch, method, properties, body):
        try:
            client_id = properties.headers.get('clientId')
            if not client_id:
                logger.error("clientId not found in headers")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return None
                
            body_str = body.decode('utf-8')
            return Message(body_str, client_id, method.delivery_tag, ch)
        except Exception as e:
            logger.error(f"Error creating message: {e}")
            return None

    def consume(self, auto_ack, callback):
        def message_callback(ch, method, properties, body):
            message = self.message_from_delivery(ch, method, properties, body)
            if message:
                callback(message)
            
        return self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=message_callback,
            auto_ack=auto_ack
        )

    def signal_handler(self, signum, frame):
        logger.info("Received SIGTERM signal, closing connection")
        self.close()
    
    def publish(self, body):
        encoded_body = body.encode('utf-8')
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.queue_name,
            body=encoded_body
        )
    
    def start_consuming(self):
        self.channel.start_consuming()

    def close(self):
        self.channel.stop_consuming()
        self.channel.close()

class ProducerQueue:
    def __init__(self, connection, name, next_replicas):
        self.channel = connection.channel()
        self.next_replicas = next_replicas
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.channel.exchange_declare(
            exchange=name,
            exchange_type='direct',
            durable=False
        )

        self.exchange_name = name
        self.queue_name = name
        self.bound_queues = set()

    def publish(self, body, client_id, movie_id=None):
        if movie_id:
            routing_key = str(hash_string(movie_id, self.next_replicas))
        else:
            routing_key = "1"
            
        self._ensure_queue_exists(routing_key)
        
        headers = {'clientId': client_id}
        
        if isinstance(body, str):
            body = body.encode('utf-8')
            
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type='text/plain',
                headers=headers
            )
        )
        
    def _ensure_queue_exists(self, routing_key):
        if routing_key not in self.bound_queues:
            queue_name = f"{self.exchange_name}_{routing_key}"
            
            self.channel.queue_declare(
                queue=queue_name,
                durable=False
            )
            
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=queue_name,
                routing_key=routing_key
            )
            
            self.bound_queues.add(routing_key)

    def publish_finished(self, client_id):
        app_id = os.environ.get('ID', '1')
        for i in range(1, self.next_replicas + 1):
            logger.info(f"Publishing FINISHED for client {client_id} with routing key {i}")
            finished_msg = f"FINISHED:{app_id}"
            
            self._ensure_queue_exists(str(i))
            
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=str(i),
                body=finished_msg.encode('utf-8'),
                properties=pika.BasicProperties(
                    content_type='text/plain',
                    headers={'clientId': client_id}
                )
            )

    def signal_handler(self, signum, frame):
        logger.info("Received SIGTERM signal, closing connection")
        self.channel.close()
