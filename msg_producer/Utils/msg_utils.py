
# Utilities
from Utils.db_utils import connect_to_db
from datetime import datetime
from psycopg2 import sql
import random


class GenerateMsg():
    """
    A class to generate synthetic messages with different categories.
    
    This class creates structured message objects that simulate real-world messages,
    with controllable distribution between "signal" (important) categories and 
    "noise" (background) categories. Each message includes metadata like location,
    timestamps, and unique identifiers along with generated text content.
    """
    def __init__(
        self, 
        templates: dict[str], 
        noise_categ: dict[str], 
        signal_categ: dict[str],
        synonymus: dict[str],
        msg_lat: float, 
        msg_long: float,
        macro_i: int,
        micro_i: int
    ) -> None:
        """
        Initialize the message generator with necessary parameters.
        
        Parameters:
            templates (dict[str]): Dictionary of message templates keyed by category
            noise_categ (dict[str]): Categories considered as "noise" or background data
            signal_categ (dict[str]): Categories considered as "signal" or important data
            synonymus (dict[str]): Dictionary of word variations for template filling
            msg_lat (float): Latitude coordinate for the message
            msg_long (float): Longitude coordinate for the message
            macro_i (int): Macro area identifier number
            micro_i (int): Micro area identifier number
        """
        self.templates = templates
        self.synonymus = synonymus
        self.noise_categ = noise_categ
        self.signal_categ = signal_categ
        self.msg_lat = msg_lat
        self.msg_long = msg_long
        self.macro_i = macro_i
        self.micro_i = micro_i

    def generate(self) -> dict:
        """
        Generate a complete message including content and metadata.
        
        This method orchestrates the message creation process by:
        1. Selecting a message category (signal or noise)
        2. Generating text content based on the selected category
        3. Creating metadata including timestamps and unique identifiers
        4. Assembling all components into a structured message dictionary
        
        Returns:
            dict: A complete message object with text and metadata
        """
        category = self._select_category()
        text = self._generate_text(category)

        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        macroarea_id = f"A{self.macro_i}"
        microarea_id = f"M{self.micro_i}"
        unique_msg_id = f"{macroarea_id}-{microarea_id}_{timestamp.replace('T', '_')}"

        message = {
            "unique_msg_id": unique_msg_id,
            "macroarea_id": macroarea_id,
            "microarea_id": microarea_id,
            "text": text,
            "latitude": self.msg_lat,
            "longitude": self.msg_long,
            "timestamp": timestamp
        }

        return message
        
    def _select_category(self):
        return random.choices(
            self.signal_categ + self.noise_categ,
            weights=[70/len(self.signal_categ)] * len(self.signal_categ) +
                    [30/len(self.noise_categ)] * len(self.noise_categ)
        )[0]

    def _fill_template(self, template:str) -> str:
        """
        Fills a message template with randomly selected synonymus.

        Parameters:
        - TEMPLATES (str): A message template string containing placeholders (e.g., "{disaster}", "{urgency}").

        Returns:
        - str: The template with all placeholders replaced by randomly selected synonyms from the SYNONYMS dictionary.
        """
        # Find all placeholders in the template and replace them with random choices from synonymus
        return template.format(**{key: random.choice(values) for key, values in self.synonymus.items() if f"{{{key}}}" in template})

    def _generate_text(self, category: str) -> str:
        """
        Generates a synthetic message text for a given category by selecting a random template and filling it with synonyms.

        Parameters:
        - category (str): The category of the message (e.g., "help_request", "damage_report").

        Returns:
        - str: A fully formed message string, either filled using a template or a fallback placeholder if the category is unknown.
        """
        if category in self.templates:
            template = random.choice(self.templates[category])
            return self._fill_template(template)
        else:
            return f"This is a placeholder message for category '{category}'."


def fetch_micro_bbox_from_db(macro_i: int, micro_i) -> tuple | None:
    """
    Retrieves the bounding box of a specific microarea within a macroarea from the database.

    Args:
        macro_i (int): The index of the macroarea (e.g., 1 for 'A1').
        micro_i (int): The index of the microarea (e.g., 25 for 'A1-M25')

    Returns:
        tuple or None: A tuple of (min_long, min_lat, max_long, max_lat) if a bounding box is found,
                       or None if the macroarea or microarea does not exist.
    """
    conn = connect_to_db()
    cur = conn.cursor()

    table_name = f"microareas"
    microarea_id = f"A{macro_i}-M{micro_i}"

    # Step 3: Fetch bounding box from microarea table
    cur.execute(sql.SQL("""
        SELECT min_long, min_lat, max_long, max_lat
        FROM {}
        WHERE microarea_id = %s
    """).format(sql.Identifier(table_name)), (microarea_id,))

    bbox = cur.fetchone()

    cur.close()
    conn.close()

    if bbox is None:
        raise ValueError(f"[ERROR] No bounding box found for microarea '{microarea_id}' in table '{table_name}'. This indicates a data integrity issue in your DB population process.")

    return bbox, microarea_id

