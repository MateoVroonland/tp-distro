import csv
import socket
import logging
from io import StringIO
import time

from internal.utils.communication import CompleteSocket
from internal.utils.csv_formatters import process_credits_row, process_movies_row

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 2 * 1024 * 1024
SERVER_HOST="requesthandler"
SERVER_PORT=8888

def create_tcp_connection(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    complete_sock = None
    try:
        sock.connect((host, port))
        complete_sock = CompleteSocket(sock)
        return complete_sock
    except (ConnectionError, OSError) as e:
        logger.error(f"Failed to connect to {host}:{port} - {str(e)}")
        if complete_sock:
            complete_sock.close()
        return None
    
def create_batch_from_csv(file_path):
    current_batch = ""
    current_batch_size = 0
    file_name = file_path.split("/")[-1]
    file_processor = None
    
    if "movies.csv" in file_name:
        file_processor = process_movies_row
        logger.info("Processing movies.csv file")
    elif "credits.csv" in file_name:
        file_processor = process_credits_row
        logger.info("Processing credits.csv file")
    else:
        logger.info(f"No specific processor for {file_name}, sending raw data")
    
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file) 
        for row in csv_reader:
            logger.info(f"Processing row {row}")
            if not row:
                continue
            if file_processor:
                row = file_processor(row)
                
            output = StringIO()
            csv_writer = csv.writer(output)
            csv_writer.writerow(row)
            encoded_csv_row = output.getvalue()

            line_size = len(encoded_csv_row.encode('utf-8'))
            
            if current_batch_size + line_size > BATCH_SIZE:

                yield current_batch
                current_batch = ""
                current_batch_size = 0
            
            current_batch += encoded_csv_row
            current_batch_size += line_size
    
    if current_batch_size > 0:
        yield current_batch

def send_file(complete_sock, file_path):
    logger.info(f"Starting to send file: {file_path}")
    batch_count = 0
    # complete_sock.send_all(f"START_FILE:{file_path}")
    try:
        for batch in create_batch_from_csv(file_path):
            batch_count += 1
            complete_sock.send_all(f"FILE_BATCH:{file_path}:{batch}")
            logger.info(f"Sent ----> FILE_BATCH:{file_path}:{batch}")
        
        sent = complete_sock.send_all(f"FINISHED_FILE:{file_path}")
        logger.info(f"Sent ----> FINISHED_FILE:{file_path}")
        logger.info(f"Completed sending file {file_path} with {batch_count} batches")
        return True    
    except (ConnectionError, OSError) as e:
        logger.error(f"Failed to send file {file_path}: {str(e)}")

def get_backoff_seconds(i):
    if i < 10:
        return 20
    elif i < 20:
        return 40
    else:
        return 60 * 2
    


def wait_for_results(complete_sock, id):  
    tries = 0
    received_results = False
    data = None
    try:
        while not received_results:
            try:
                complete_sock.send_all(f"{id}:REQUEST_RESULTS")
                data = complete_sock.recv_all().decode('utf-8')
                if not data:
                    logger.info("Server closed the connection, all results received")
                    break
                
                if data == "NO_RESULTS":
                    backoff_seconds = get_backoff_seconds(tries)
                    logger.info(f"No results received, retrying in {backoff_seconds if backoff_seconds < 60 else backoff_seconds / 60} {'seconds' if backoff_seconds < 60 else 'minutes'}")
                    time.sleep(backoff_seconds)
                    tries += 1
                    continue
                else:
                    received_results = True

            except ConnectionError:
                logger.info("Server closed the connection")
                break
    except (ConnectionError, OSError) as e:
        logger.error(f"Error while receiving results: {str(e)}")
        return None
    finally:
        if complete_sock:
            complete_sock.close()
    return data
def initialize_connection():
    complete_sock = create_tcp_connection(SERVER_HOST, SERVER_PORT)
    if not complete_sock:
        logger.error("Failed to create TCP connection")
        return None
    complete_sock.send_all("INITIALIZE_CONNECTION")
    ack = complete_sock.recv_all().decode('utf-8')
    logger.info("Received ACK: %s", ack)

    if not ack.startswith("ACK"):
        logger.error("Failed to initialize connection")
        return None
    id = ack.split(":")[1].strip()
    
    logger.info("Initialized connection with ID: %s", id)
    return complete_sock, id

def main():
    logger.info("Starting client")
    complete_sock, id = initialize_connection()
    if not complete_sock:
        complete_sock.close()
        logger.error("Failed to initialize connection")
        return
    files = [
        {"path": "/docs/movies.csv"},
        {"path": "/docs/credits.csv"},
        {"path": "/docs/ratings.csv"}
    ]
    logger.info("Sending files")
    
    for file_info in files:
        file_path = file_info["path"]
        if send_file(complete_sock, file_path):
            logger.info(f"File {file_path} sent successfully")

    logger.info("All files sent, sleeping for 5 minutes")
        

    logger.info("Waiting for results")

    data = wait_for_results(complete_sock, id)

    print(data)

    logger.info("Client execution completed")
    complete_sock.close()

if __name__ == "__main__":
    main()
