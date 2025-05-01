
# Utilities
import random
from psycopg2 import sql
from geo_utils import plot_image
from db_utils import connect_to_db

from sentinelhub import (
    SHConfig,
    DataCollection,
    SentinelHubRequest,
    BBox,
    bbox_to_dimensions,
    CRS,
    MimeType,
)


# |--- UTILITY FUNCTIONS FOR SENTINEL REQUESTS ---|

def get_aoi_bbox_and_size(bbox, resolution=10):
    """
    Converts a bounding box in WGS84 format into a SentinelHub-compatible BBox object
    and computes the corresponding image size in pixels at the given spatial resolution.

    This function is typically used to prepare an area of interest (AOI) for a SentinelHub
    request by wrapping the bounding box and computing the width and height (in pixels)
    based on the desired resolution in meters.

    Args:
        bbox (list or tuple): A list or tuple in the format [min_long, min_lat, max_long, max_lat].
        resolution (int, optional): Spatial resolution in meters per pixel. Default is 10.

    Returns:
        tuple:
            - BBox: A SentinelHub BBox object in CRS.WGS84 format.
            - tuple: Image size as (width, height) in pixels.
    """
    aoi_bbox = BBox(bbox=bbox, crs=CRS.WGS84)
    aoi_size = bbox_to_dimensions(aoi_bbox, resolution=resolution)
    print(f"[INFO] Image shape at {resolution} m resolution: {aoi_size} pixels")

    return aoi_bbox, aoi_size


def true_color_image_request_processing(aoi_bbox,
                     aoi_size,
                     config,
                     start_time_single_image = "2024-05-01",
                     end_time_single_image = "2024-05-20"):
    """
    Creates a SentinelHubRequest object to retrieve a true color satellite image
    from the Sentinel-2 Level-2A data collection for a given area of interest (AOI)
    and time interval.

    This function builds a request using an evalscript that combines the red (B04),
    green (B03), and blue (B02) bands to produce a true color image, and sets up
    the appropriate parameters for resolution, time, and mosaicking strategy.

    Args:
        aoi_bbox (BBox): A SentinelHub BBox object defining the area of interest in WGS84.
        aoi_size (tuple): Image size as (width, height) in pixels, typically computed from resolution.
        config (SHConfig): Configuration object with SentinelHub credentials and settings.
        start_time_single_image (str): Start date of the time interval (format 'YYYY-MM-DD').
        end_time_single_image (str): End date of the time interval (format 'YYYY-MM-DD').

    Returns:
        SentinelHubRequest: A request object ready to be executed with `.get_data()` to fetch the image.
    """
    evalscript_true_color = """
    //VERSION=3

    function setup() {
        return {
            input: [{
                bands: ["B02", "B03", "B04"]
            }],
            output: {
                bands: 3
            }
        };
    }

    function evaluatePixel(sample) {
        return [sample.B04, sample.B03, sample.B02];
    }
    """

    request_true_color = SentinelHubRequest(
        evalscript=evalscript_true_color,
        input_data=[
            SentinelHubRequest.input_data(
                data_collection=DataCollection.SENTINEL2_L2A.define_from(
                    name="s2l2a", service_url="https://sh.dataspace.copernicus.eu"
                ),
                time_interval=(start_time_single_image, end_time_single_image),
                other_args={"dataFilter": {"mosaickingOrder": "leastCC"}},
            )
        ],
        responses=[SentinelHubRequest.output_response("default", MimeType.PNG)],
        bbox=aoi_bbox,
        size=aoi_size,
        config=config,
    )

    return request_true_color


def fetch_bbox_from_db(i):
    """
    Retrieves the bounding box of a specific microarea within a macroarea from the database.

    This function connects to the PostgreSQL database, looks up how many microareas are stored
    for the given macroarea (identified by index `i`), and fetches the bounding box
    (min_long, min_lat, max_long, max_lat) of one of its microareas.

    Note:
        Currently, the function selects a microarea at random for demonstration purposes.
        In a full implementation, this should be replaced with a deterministic selection
        (e.g., based on input parameters, spatial query, or area of interest).

    Args:
        i (int): The index of the macroarea whose microareas are stored in the database.

    Returns:
        tuple or None: A tuple of (min_long, min_lat, max_long, max_lat) if a bounding box is found,
                       or None if the macroarea does not exist in the metadata table.
    """
    conn = connect_to_db()
    cur = conn.cursor()

    # Fecth a random bounding box from the database for the current macroarea
    cur.execute(sql.SQL("""
        SELECT numof_microareas 
        FROM n_microareas
        WHERE macro_area_num = %s             
    """), (i,))

    n_microareas= cur.fetchone()
    if n_microareas is None:
        return None  # No such macro_area_num in the table

    n_example = random.randint(1, n_microareas[0])

    table_name = f"macro_area_{i}"
    example_query = sql.SQL("""
        SELECT min_long, min_lat, max_long, max_lat
        FROM {}
        WHERE micro_area_num = %s
    """).format(sql.Identifier(table_name))

    cur.execute(example_query, (n_example,))
    fetch_bbox = cur.fetchone()

    cur.close()
    conn.close()
    
    return fetch_bbox


def display_image(requested_data):
    """
    Displays a true color image returned by a SentinelHub request.

    This function assumes the input list always contains exactly one image (as returned
    by SentinelHubRequest.get_data()). It extracts that image and visualizes it using
    a helper plotting function with brightness adjustment and clipping.

    Notes:
        - The image is rescaled from 0–255 to 0–1 using a factor of 1/255.
        - A brightness factor of 3.5 is applied to enhance visibility.
        - The display is clipped to the [0, 1] range to avoid overexposure.

    Args:
        requested_data (list): A single-element list containing one image array (e.g., a NumPy array)
                               representing the true color Sentinel-2 image.
    """
    if not requested_data:
        print("No image data to display.")
        return
    image = requested_data[0]
    # plot function
    # factor 1/255 to scale between 0-1
    # factor 3.5 to increase brightness
    plot_image(image, factor=3.5 / 255, clip_range=(0, 1))


def main():
    
    # Init Client Configuration
    config = SHConfig()

    # Fetch image for each macroarea
    n_of_macroareas = 7
    for i in range(1, n_of_macroareas + 1):

        # Fetch bbox example for current macroarea
        microarea_example_bbox = fetch_bbox_from_db(i)
        if microarea_example_bbox is None:
            print(f"[WARNING] No bounding box found for macroarea {i}, skipping.")
            continue

        # Bbox to correct format
        curr_aoi_coords_wgs84 = list(microarea_example_bbox)

        # Create objects to use in  SentinelHubRequest.DataCollections
        resolution = 10
        curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(curr_aoi_coords_wgs84,
                                                             resolution = resolution)

        # Send HTTP request to Copernicus EU server through RESTfull APIs
        start_time = "2024-05-01"
        end_time = "2024-05-20"
        request_true_color = true_color_image_request_processing(curr_aoi_bbox,
                                                    curr_aoi_size,
                                                    config,
                                                    start_time,
                                                    end_time)
        # Get data from the request
        true_color_imgs = request_true_color.get_data()
        if not true_color_imgs:
            print(f"[WARNING] No image data returned for macroarea {i}.")
            continue
        
        # Print infos about img
        print(
        f"[INFO] Returned data is of type = {type(true_color_imgs)} and length {len(true_color_imgs)}."
        )
        print(
        f"[INFO] Single element in the list is of type {type(true_color_imgs[-1])} and has shape {true_color_imgs[-1].shape}"
        )

        # Process given image
        display_image(true_color_imgs)


if __name__ == "__main__":
    main()

