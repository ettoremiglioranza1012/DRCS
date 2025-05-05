# Utilities
from sentinelhub import SHConfig
from kafka import KafkaProducer
import logging
import time

from imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
    fetch_micro_bbox_from_db
)


# Logs Configuration
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def stream_macro1():
    config = SHConfig()
    i = 5
    stream = True
    print("\n")
    logger.info(f"Streaming data for macroarea_A{i}...\n")
    iteration_time = None

    while stream:
        try:
            t_macro_start = time.perf_counter()

            logger.info("Fetching bounding box from DB...")
            microarea_example_bbox = fetch_micro_bbox_from_db(i)
            if microarea_example_bbox is None:
                logger.error(f"No bounding box found for macroarea {i}, skipping.")
                break

            curr_aoi_coords_wgs84 = list(microarea_example_bbox)

            logger.info("Converting BBox and calculating image size...")
            t_bbox = time.perf_counter()
            curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(
                curr_aoi_coords_wgs84, resolution=10)
            logger.info("BBox ready in %.2f s", time.perf_counter() - t_bbox)

            # Date interval
            start_time = "2024-05-01"
            end_time = "2024-05-20"

            logger.info("Building SentinelHub request...")
            t_req = time.perf_counter()
            request_true_color = true_color_image_request_processing(
                curr_aoi_bbox, curr_aoi_size, config, start_time, end_time)
            logger.info("Request created in %.2f s", time.perf_counter() - t_req)

            logger.info("⬇Downloading image data from SentinelHub...")
            t_dl = time.perf_counter()
            true_color_imgs = request_true_color.get_data()
            logger.info("Data fetched in %.2f s", time.perf_counter() - t_dl)

            if not true_color_imgs:
                logger.warning("No image data returned. Skipping.")
                continue

            logger.info("Image np.ndarray shape: %s", true_color_imgs[0].shape)
            logger.info("Processing image...\n")
            t_proc = time.perf_counter()
            img_payload_str = process_image(true_color_imgs)
            logger.info("Image processed in %.2f s", time.perf_counter() - t_proc)
            
            iteration_time = time.perf_counter() - t_macro_start
            logger.info("Total macroarea cycle time: %.2f s", iteration_time)

        except Exception as e:
            logger.error("Error occured during streaming cycle: %s", str(e))
            logger.warning("Skip to next fetch...")
            continue
        
        finally:
            min_cycle_duration = 60 # seconds
            if iteration_time is None:
                elapsed = time.perf_counter() - t_macro_start
            else:
                elapsed = iteration_time
            
            remaining_time = max(0, min_cycle_duration-elapsed)
            if remaining_time > 0:
                logger.info("Waiting %.2f s before next image fetching...\n\n", remaining_time)
                time.sleep(remaining_time)

        # Uncomment this if/when Kafka is integrated
        """
        producer = KafkaProducer(bootstrap_servers=['localhost:29092'])
        value = img_payload_str.encode('utf8')
        future = producer.send(f'satellite_imgs_A{i}', value=value)
        """

if __name__ == "__main__":
    stream_macro1()

