
# Utilities
from Utils.geo_sensor_utils import *
import logging
import json
import os 


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def stream_micro_sens(macroarea_i: int, microarea_i:int) -> None:
    """
    Insert comment
    """
    print("\n[STREAM-PROCESS]")
    
    # Initialize Kafka Producer

    # Set up data stream parameters
    stream = True   # Stream until False

    print("\n")
    logger.info(f"Streaming data for microarea_A{macroarea_i}-M{microarea_i}...\n")

    while stream:
        try:
            logger.info("Fetching microarea sensor stations information...")
            n_stats = get_number_of_stats(macroarea_i, microarea_i)
            logger.info("Fetching completed sucessfully.")

            if not n_stats > 49 or not n_stats:
                raise SystemError(f"Number of stations inconsistent: {n_stats}, check data integrity.")
            
            # Generate fake measurements for i-th station in microarea
            list_of_mesdict = list()
            logger.info(f"Fetching measurements for each stations in microarea: 'A{macroarea_i}-{microarea_i}'")
            for i in range(n_stats):
                temp_mes = generate_measurements_json(i+1, microarea_i, macroarea_i)
                list_of_mesdict.append(temp_mes)
                if not temp_mes:
                    raise ValueError(f"Measurements for 'S_A{macroarea_i}-M{microarea_i}_{i:03}' not consistent, check 'generate_measurements_json()' function.")
            
            # Dump data inside message with json format
            message = list_of_mesdict
            if not message or not len(message) > 49:
                raise ValueError("Message not consistent, check data integrity.")
            logger.info("Fetching completed sucessfully.")

        except Exception as e:
            raise SystemError(f"Error during streaming procedure: {e}")


"""
            if not os.path.isfile('messages.json'):
                with open('messages.json', 'w') as f:
                    # '.dump' create instead an fie-like object. 
                    json.dump(message, f, indent=4) # I the file doesn't exist, create it and dump the message.
            else:
                with open('messages.json', 'r+', encoding='utf-8') as f:
                    try:
                        data = json.load(f)         # Load the already existing list of dict.
                    except json.JSONDecodeError:
                        data = []                   # If the file is corrupted, create an empty list.
                    
                    data.append(message)            # Append last dict/message.
                    
                    f.seek(0)                       # Move the cursor back to the start of the file, so it can be overwritten from the beginning. 
                    
                    json.dump(data, f, indent=4)    # Serializes the updated list back into JSON format and writes it to the file, starting at the top.
                    
                    f.truncate()                    # Trims the file after the current write position — this is important!
                                                    # If the new content is shorter than the old one, it avoids leaving trailing junk from the old data.
                                                    # It shouldn't happen cause we have sequential messages, but just in case we put it.
"""