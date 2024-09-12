# dicts with heatmap for every_poicategory
# 6 heatmaps for every poi category walking: gussian, combined_gaussian, closest_abverage
poi_dicts = {
    # Immediate Surroundings (travel time (tt) ≤ 5 min)
    'bus_stop_gtfs': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    'childcare': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    'grocery_store': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    'school_isced_level_1': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 300000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 1
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    # Close Surroundings (tt ≤ 10 min)
    'pharmacy': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 600000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 600000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 500000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 2
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 500000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 2
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    'rail_station_gtfs': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 600000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 600000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 500000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 2
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 500000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 2
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    # District-Wide Surroundings (tt ≤ 20 min)
    'general_practitioner': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1100000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1100000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 550000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 5
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 550000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 5
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    'restaurant': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1100000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1100000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 550000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 5
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 550000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 5
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    # Citywide Surroundings (tt > 20 min)
    'museum': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1800000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1800000,
                    "destination_potential_column": None
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 670000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 7
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 670000,
                    "destination_potential_column": None,
                    "static_travel_time_component": 7
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        }
    },
    'population': {
        'heatmap_walking_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1800000,
                    "destination_potential_column": 'bigint_attr1'
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_gaussian': {
            "impedance_function": "gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 1800000,
                    "destination_potential_column": 'bigint_attr1'
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 670000,
                    "destination_potential_column": 'bigint_attr1',
                    "static_travel_time_component": 7
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_combined_gaussian': {
            "impedance_function": "combined_gaussian",
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "sensitivity": 670000,
                    "destination_potential_column": 'bigint_attr1',
                    "static_travel_time_component": 7
                }
            ],
            "routing_type": "bicycle"
        },
        'heatmap_walking_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "walking"
        },
        'heatmap_bicycle_closest_average': {
            "opportunities": [
                {
                    "max_traveltime": 30,
                    "number_of_destinations": 1
                }
            ],
            "routing_type": "bicycle"
        },
    },
}