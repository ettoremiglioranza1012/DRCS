
# Utilities
import random
import json

from geo_utils import (
    read_json,
    write_json,
    polygon_to_bbox,
    create_microareas_grid,
    plot_image,
    dict_to_polygon,
)

from sentinelhub import (
    SHConfig,
    DataCollection,
    SentinelHubCatalog,
    SentinelHubRequest,
    BBox,
    bbox_to_dimensions,
    CRS,
    MimeType,
)


# |--- UTILITY FUNCTIONS FOR SENTINEL REQUESTS ---|

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

    n_of_macroareas = 7
    for i in range(1, n_of_macroareas+1):


        # |--- MACROAREA COORDINATES READING ---|

        path_to_current_geoJson_macro = f"Satellite/Macro_input/macroarea_{i}.json"
        macro_geom = read_json(path_to_current_geoJson_macro)
        
        
        # |--- MICROAREA GRID SETUP & SELECTION ---|
        
        # Create grid for macro
        macro_bbox = polygon_to_bbox(macro_geom)
        microareas_bbox_dict, n_microareas = create_microareas_grid(macro_bbox, 500, i)

        # Extract a micro area example out of n_microareas
        microarea_n_example = random.randint(1, n_microareas)
        microarea_image_name = f'macroarea_{i}_micro_{microarea_n_example}'
        microarea_example_bbox = microareas_bbox_dict[microarea_image_name]
        
        # Translate bbox into correct format
        curr_aoi_coords_wgs84 = list(microarea_example_bbox)

        # Create objects to use in  SentinelHubRequest.DataCollections
        resolution = 10
        curr_aoi_bbox, curr_aoi_size = get_aoi_bbox_and_size(curr_aoi_coords_wgs84,
                                                             resolution = resolution)
        

        # |--- APPROXIMATE MACROGRID RECONSTRUCTION (Copernicus Browser-Compatible) ---|
        
        # This is useful to check if the created grid is as expected
        macrogrid_outcome_polygon = dict_to_polygon(microareas_bbox_dict)
        macrogrid_outcome_path = f"Satellite/Macro_output/macroarea_{i}.json"
        write_json(macrogrid_outcome_path, macrogrid_outcome_polygon)
        
        
        # |--- SENTINEL-HUB IMAGE REQUEST & DOWNLOAD ---|
        
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

        # Print infos about img
        print(
        f"Returned data is of type = {type(true_color_imgs)} and length {len(true_color_imgs)}."
        )
        print(
        f"Single element in the list is of type {type(true_color_imgs[-1])} and has shape {true_color_imgs[-1].shape}"
        )
        

        # |--- IMAGE PROCESSING ---|

        # Process given image
        # display_image(true_color_imgs, microarea_image_name)


if __name__ == "__main__":
    main()




# |--- CATALOG SEARCH FOR GEODATA ---|
"""
# For a given time_range, get all .SAFE object (geoFile)
# This is useful to understand volumes of geodata
# Not sure we will use this
start_time = "2024-04-01"
end_time = "2024-04-20"
results = get_catalog_search(curr_aoi_coords_wgs84, config
true_color_imgs = request_true_color.get_data()
"""