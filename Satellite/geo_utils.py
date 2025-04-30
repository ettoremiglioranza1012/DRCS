
# Utilities
import matplotlib.pyplot as plt
import numpy as np
import math
import json
import os


# |---Utils functions---|

def read_json(macroarea_input_path):
    """
    Reads a JSON file and returns the loaded Python dictionary.

    Parameters:
    - filepath: str, path to the .json file

    Returns:
    - dict: The JSON content as a Python dictionary
    """
    with open(macroarea_input_path, 'r') as f:
        data = json.load(f)
    return data


def write_json(macroarea_output_path, poly):
    """
    Scrive un oggetto GeoJSON (es. un poligono) su file in formato JSON leggibile.

    Args:
        macroarea_output_path (str): percorso del file di output, incluso il nome e l'estensione (.geojson o .json).
        poly (dict): dizionario che rappresenta un oggetto GeoJSON (tipicamente un 'Polygon').
    """
    with open(macroarea_output_path, "w") as w:
        json.dump(poly, w, indent=4)


def polygon_to_bbox(geom):
    """
    Converte un GeoJSON Polygon in una bounding box (min_lon, min_lat, max_lon, max_lat).
    
    Parameters:
    - geom (dict): dizionario GeoJSON contenente un poligono
    
    Returns:
    - tuple: bounding box (min_lon, min_lat, max_lon, max_lat)
    """
    if geom["type"] != "Polygon" or geom[""]:
        raise ValueError("Solo poligoni supportati.")

    coords = geom["coordinates"][0]  # lista di coordinate [lon, lat]

    lons = [point[0] for point in coords]
    lats = [point[1] for point in coords]

    min_lon = min(lons)
    max_lon = max(lons)
    min_lat = min(lats)
    max_lat = max(lats)

    return (min_lon, min_lat, max_lon, max_lat)


def create_microareas_grid(bbox, max_area_km2, macro_area_n):
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

            microareas[f"macroarea_{macro_area_n}_micro_{count}"] = (
                cell_min_lon,
                cell_min_lat,
                cell_max_lon,
                cell_max_lat
            )
            count += 1

    return microareas, count


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


def dict_to_polygon(microdict):
    """
    Riceve un dizionario di microaree (con bbox) e restituisce il poligono GeoJSON
    che rappresenta il bounding box complessivo della macroarea.

    Args:
        microdict (dict): chiavi tipo 'micro_1', 'micro_2', ... con valori (min_lon, min_lat, max_lon, max_lat)

    Returns:
        dict: oggetto GeoJSON Polygon
    """
    min_lon = float('inf')
    min_lat = float('inf')
    max_lon = float('-inf')
    max_lat = float('-inf')

    for _, (lon_min, lat_min, lon_max, lat_max) in microdict.items():
        min_lon = min(min_lon, lon_min)
        min_lat = min(min_lat, lat_min)
        max_lon = max(max_lon, lon_max)
        max_lat = max(max_lat, lat_max)

    # Costruzione del poligono bounding box
    polygon = {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, max_lat],
            [max_lon, max_lat],
            [max_lon, min_lat],
            [min_lon, min_lat],
            [min_lon, max_lat]  # chiusura del poligono
        ]]
    }

    return polygon

