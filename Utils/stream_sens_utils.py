
# Utilities 
import logging

# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def stream_macro_stats(macroarea_i: int, microarea_i:int) -> None:
    """
    Insert comment
    """
    print("\n[STREAM-PROCESS]\n")
    
    # Initialize Kafka Producer

    # Set up data stream parameters
    stream = True   # Stream until False

    print("\n")
    logger.info(f"Streaming data for microarea_A{macroarea_i}-M{microarea_i}...\n")

    while stream:
        try:
            
            logger.info("Fetching microarea sensor stations information...")




