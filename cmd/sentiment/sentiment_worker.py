import logging
import pika

from internal.sentiment.sentiment_worker import SentimentWorker
from internal.utils.queue import new_queue


def main():
    logging.info("Starting sentiment worker...")
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    
    input_queue = new_queue(connection, 'movies_metadata_q5')
    output_queue = new_queue(connection, 'movies_sentiment_processed')
    
    worker = SentimentWorker(input_queue, output_queue)
    worker.start()
    
    connection.close()


if __name__ == "__main__":
    main()