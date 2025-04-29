# Import Meteostat library
from meteostat import Stations

# Get nearby weather stations
stations = Stations().nearby(46.07, 11.12).fetch(4)  # Trento!
stations.reset_index().to_json("trento_weather.json", orient="records", lines=True)

