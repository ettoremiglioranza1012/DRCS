
# Utilities
import matplotlib.pyplot as plt
import numpy as np
import math
import json
import random
import os

from sentinelhub import (
    SHConfig,
    DataCollection,
    SentinelHubCatalog,
    SentinelHubRequest,
    SentinelHubStatistical,
    BBox,
    bbox_to_dimensions,
    CRS,
    MimeType,
    Geometry,
)


# |---Utils functions---|


def read_json(macroarea_path):
    """
    Reads a JSON file and returns the loaded Python dictionary.

    Parameters:
    - filepath: str, path to the .json file

    Returns:
    - dict: The JSON content as a Python dictionary
    """
    with open(macroarea_path, 'r') as f:
        data = json.load(f)
    return data


def polygon_to_bbox(geom):
    """
    Converte un GeoJSON Polygon in una bounding box (min_lon, min_lat, max_lon, max_lat).
    
    Parameters:
    - geom (dict): dizionario GeoJSON contenente un poligono
    
    Returns:
    - tuple: bounding box (min_lon, min_lat, max_lon, max_lat)
    """
    if geom["type"] != "Polygon":
        raise ValueError("Solo poligoni supportati.")

    coords = geom["coordinates"][0]  # lista di coordinate [lon, lat]

    lons = [point[0] for point in coords]
    lats = [point[1] for point in coords]

    min_lon = min(lons)
    max_lon = max(lons)
    min_lat = min(lats)
    max_lat = max(lats)

    return (min_lon, min_lat, max_lon, max_lat)


def create_microareas_grid(bbox, max_area_km2):
    """
    Divide una bounding box in microaree rettangolari di dimensione massima circa max_area_km2.

    Args:
        bbox (tuple): (min_lon, min_lat, max_lon, max_lat)
        max_area_km2 (float): area massima per microarea in km².

    Returns:
        dict: dizionario con chiavi 'micro_1', 'micro_2', ..., e valori bbox (min_lon, min_lat, max_lon, max_lat)
    """
    min_lon, min_lat, max_lon, max_lat = bbox

    # Calcolo latitudine media per correggere la lunghezza dei gradi di longitudine
    mean_lat = (min_lat + max_lat) / 2
    km_per_deg_lat = 111  # circa costante
    km_per_deg_lon = 111 * math.cos(math.radians(mean_lat))

    # Dimensioni della bounding box in km
    width_km = (max_lon - min_lon) * km_per_deg_lon
    height_km = (max_lat - min_lat) * km_per_deg_lat

    total_area_km2 = width_km * height_km

    # Numero stimato di microaree
    num_microareas = math.ceil(total_area_km2 / max_area_km2)

    # Calcolo righe e colonne approssimative
    n_cols = math.ceil(math.sqrt(num_microareas * (width_km / height_km)))
    n_rows = math.ceil(num_microareas / n_cols)

    lon_step = (max_lon - min_lon) / n_cols
    lat_step = (max_lat - min_lat) / n_rows

    microareas = {}
    count = 1

    for i in range(n_rows):
        for j in range(n_cols):
            cell_min_lon = min_lon + j * lon_step
            cell_max_lon = cell_min_lon + lon_step
            cell_min_lat = min_lat + i * lat_step
            cell_max_lat = cell_min_lat + lat_step

            microareas[f"micro_{count}"] = (
                cell_min_lon,
                cell_min_lat,
                cell_max_lon,
                cell_max_lat
            )
            count += 1

    return microareas, n_cols*n_rows


def plot_image(image, img_name, factor=3.5/255, clip_range=(0, 1), output_dir='Satellite/Output_images'):
    """
    Plots an RGB image after rescaling and clipping, and saves it to the output directory.

    Parameters:
    - image: np.ndarray of shape (H, W, 3)
    - img_name: str, filename to save the image as (without directory)
    - factor: multiplicative rescaling factor applied to the image
    - clip_range: tuple (min_value, max_value) to clip the image values
    - output_dir: directory where to save the image
    """
    # Apply rescaling factor
    image = image * factor

    # Clip the values to the specified range
    image = np.clip(image, clip_range[0], clip_range[1])

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Plot the image
    plt.figure(figsize=(10, 10))
    plt.imshow(image)
    plt.axis('off')

    # Save the figure
    save_path = os.path.join(output_dir, img_name)
    #plt.savefig(save_path, bbox_inches='tight', pad_inches=0)
    #print(f"✅ Image saved to: {save_path}")

    plt.show()


# |---Processing Functions---|


def get_aoi_bbox_and_size(bbox, resolution=10):
    aoi_bbox = BBox(bbox=bbox, crs=CRS.WGS84)
    aoi_size = bbox_to_dimensions(aoi_bbox, resolution=resolution)
    print(f"Image shape at {resolution} m resolution: {aoi_size} pixels")

    return aoi_bbox, aoi_size


def get_catalog_search(aoi_bbox, config, start_time = "2024-04-01", end_time = "2024-04-20"):
    
    catalog = SentinelHubCatalog(config=config)
    time_interval = start_time, end_time
    
    search_iterator = catalog.search(
        DataCollection.SENTINEL2_L2A,
        bbox=aoi_bbox,
        time= time_interval,
        fields={"include": ["id", "properties.datetime"], "exclude": []},
    )

    results = list(search_iterator)
    print("Total number of results:", len(results))

    return results


def true_color_image_request_processing(aoi_bbox,
                     aoi_size,
                     config,
                     start_time_single_image = "2024-05-01",
                     end_time_single_image = "2024-05-20"):
    
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


def display_image(requested_data, img_name):
    
    if not requested_data:
        print("No image data to display.")
        return
    image = requested_data[0]
    # plot function
    # factor 1/255 to scale between 0-1
    # factor 3.5 to increase brightness
    plot_image(image, img_name, factor=3.5 / 255, clip_range=(0, 1))


def main():
    
    # Init Client Configuration
    config = SHConfig()

    n_of_macroareas = 2 # always one more than the total of macros
    for i in range(1, n_of_macroareas):
        path_to_current_geoJson_macro = f"Satellite/Macro/macroarea_{i}.json"
        macro_geom = read_json(path_to_current_geoJson_macro)
        
        # Creating grid for macro
        macro_bbox = polygon_to_bbox(macro_geom)
        microareas_bbox_dict, n_microareas = create_microareas_grid(macro_bbox, 600)
        
        # Extracting a micro area example
        microarea_n_example = random.randint(1, n_microareas)
        microarea_image_name = f'micro_{microarea_n_example}'
        microarea_example_bbox = microareas_bbox_dict[microarea_image_name]
        
        # Translate bbox into correct format
        curr_aoi_coords_wgs84 = list(microarea_example_bbox)

        """
        # For a given time_range, get all .SAFE object (geoFile)
        # This is useful to understand volumes of geodata
        # Not sure we will use this
        start_time = "2024-04-01"
        end_time = "2024-04-20"
        results = get_catalog_search(curr_aoi_coords_wgs84, config)

        true_color_imgs = request_true_color.get_data()
        """

        resolution = 10
        curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(curr_aoi_coords_wgs84,
                                                             resolution = resolution)
        start_time_single_image = "2024-05-01"
        end_time_single_image = "2024-05-20"
        request_true_color = true_color_image_request_processing(curr_aoi_bbox,
                                                      curr_aoi_size,
                                                      config,
                                                      start_time_single_image,
                                                      end_time_single_image)
        # get data from the request
        true_color_imgs = request_true_color.get_data()

        # print statistics about imgs
        print(
        f"Returned data is of type = {type(true_color_imgs)} and length {len(true_color_imgs)}."
        )
        print(
        f"Single element in the list is of type {type(true_color_imgs[-1])} and has shape {true_color_imgs[-1].shape}"
        )

        display_image(true_color_imgs, microarea_image_name)


if __name__ == "__main__":
    main()