
# Utilities
from sentinelhub import SHConfig
from kafka import KafkaProducer

from imgfetch_utils import (
    get_aoi_bbox_and_size,
    true_color_image_request_processing,
    process_image,
    fetch_micro_bbox_from_db
    )   

def stream_macro1():
    # Init Client Configuration
    config = SHConfig()
    i = 1
    stream = True
    print(f"\n[INFO] Streaming data for macroarea_{i}...\n")

    while stream:
        # Fetch bbox example for macroarea
        microarea_example_bbox = fetch_micro_bbox_from_db(i)
        if microarea_example_bbox is None:
            print(f"[ERROR] No bounding box found for macroarea {i}, skipping.")
            break

        # Bbox to correct format
        curr_aoi_coords_wgs84 = list(microarea_example_bbox)
        
        # Create sentinelHUB objects to make request
        resolution = 10
        curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(curr_aoi_coords_wgs84,
                                                              resolution=resolution)
        
        # Set up time range
        start_time = "2024-05-01"
        end_time = "2024-05-20"
        
        # Create request to sentinel hub
        request_true_color = true_color_image_request_processing(curr_aoi_bbox,
                                                                 curr_aoi_size,
                                                                 config,
                                                                 start_time,
                                                                 end_time)
        # Fetch data
        true_color_imgs = request_true_color.get_data()
        if not true_color_imgs:
            print(f"[ERROR] No image data returned for macroarea {i}.")
            break

        print(f"[INFO] Returned data is of type = {type(true_color_imgs)} and length {len(true_color_imgs)}.")
        print(f"[INFO] Single element in the list is of type {type(true_color_imgs[-1])} and has shape {true_color_imgs[-1].shape}")
        img_payload_kafka_ready = process_image(true_color_imgs)

        # Initialize Kafka
        #producer = KafkaProducer(boostrap_severs=['localhost:1234'])



if __name__ == "__main__":
    stream_macro1()
