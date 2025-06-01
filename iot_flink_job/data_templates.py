# Threshold values
THRESHOLDS = {
            "temperature_c": 35.0,
            "humidity_percent": 80.0,
            "co2_ppm": 600.0,
            "pm25_ugm3": 12.0,
            "smoke_index": 20.0,
            "infrared_intensity": 0.2,
            "battery_voltage": 3.7
        }


# Hardcoded notifications simulation, to improve with more dynamical stuff
SENT_NOTIFICATION_TO = [
      {
        "agency": "local_fire_department",
        "delivery_status": "confirmed"
      },
      {
        "agency": "emergency_management",
        "delivery_status": "confirmed"
      }
    ]

RECOMMENDED_ACTIONS = [
      {
        "action": "deploy_fire_units",
        "priority": "high",
        "recommended_resources": ["engine_company", "brush_unit", "water_tender"]
      },
      {
        "action": "evacuate_area",
        "priority": "medium",
        "radius_meters": 3000,
        "evacuation_direction": "east"
      }
    ]

AT_RISK_ASSETS = {
      "population_centers": [
        {
          "name": "Highland Park",
          "distance_meters": 2500,
          "population": 12500,
          "evacuation_priority": "high"
        }
      ],
      "critical_infrastructure": [
        {
          "type": "power_substation",
          "name": "Highland Substation",
          "distance_meters": 1800,
          "priority": "high"
        },
        {
          "type": "water_reservoir",
          "name": "Eagle Rock Reservoir",
          "distance_meters": 3200,
          "priority": "medium"
        }
      ]
    }

ENVIRONMENTAL_CONTEXT = {
    "weather_conditions": {
      "temperature": 35.2,
      "humidity": 18.4,
      "wind_speed": 25.1,
      "wind_direction": 285.3,
      "precipitation_chance": 0.02
    },
    "terrain_info": {
      "vegetation_type": "chaparral",
      "vegetation_density": "high",
      "slope": "moderate",
      "aspect": "west-facing"
    }
  }