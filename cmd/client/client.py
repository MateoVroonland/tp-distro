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

def send_file(file_path):
    logger.info(f"Starting to send file: {file_path}")
    batch_count = 0
    complete_sock = None
    try:
        complete_sock = create_tcp_connection(SERVER_HOST, SERVER_PORT)
        if not complete_sock:
            logger.error("Failed to create TCP connection")
            return False
        
        for batch in create_batch_from_csv(file_path):
            batch_count += 1
            complete_sock.send_all(batch)
        
        sent = complete_sock.send_all("FINISHED_FILE")
        logger.info(f"Sent {sent} bytes of FINISHED_FILE")
        logger.info(f"Completed sending file {file_path} with {batch_count} batches")
        return True    
    except (ConnectionError, OSError) as e:
        logger.error(f"Failed to send file {file_path}: {str(e)}")
    finally:
        if complete_sock:
            complete_sock.close()

def wait_for_results():  
    try:
        complete_sock = create_tcp_connection(SERVER_HOST, SERVER_PORT)
        if not complete_sock:
            return None
        
        while True:
            try:
                data = complete_sock.recv_all().decode('utf-8')
                if not data:
                    logger.info("Server closed the connection, all results received")
                    break
                
                logger.info(f"Received result: {data}")
                return data
            except ConnectionError:
                logger.info("Server closed the connection")
                break
    except (ConnectionError, OSError) as e:
        logger.error(f"Error while receiving results: {str(e)}")
        return None
    finally:
        if complete_sock:
            complete_sock.close()

def main():
    files = [
        {"path": "/docs/movies.csv"},
        {"path": "/docs/credits.csv"},
        {"path": "/docs/ratings.csv"}
    ]
    
    # for file_info in files:
    #     file_path = file_info["path"]
        
    if send_file(files[0]["path"]):
        logger.info(f"File {files[0]['path']} sent successfully")
        

    time.sleep(60 * 7)
    logger.info("Waiting for results")
    data = wait_for_results()
    while data is None or data == "NO_RESULTS":
        data = wait_for_results()
    logger.info("Client execution completed")

if __name__ == "__main__":
    main()
