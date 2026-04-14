def ms_to_kmh(value):
    return value * 3.6


def normalize_timestamp(timestamp):

    timestamp = timestamp.replace("Z", "")
    return timestamp


def wind_direction_to_degrees(value):
    directions = {
        "N": 0,
        "NE": 45,
        "E": 90,
        "SE": 135,
        "S": 180,
        "SW": 225,
        "W": 270,
        "NW": 315,
    }
    if value is not None and value in directions:
        return directions[value]
    return None
