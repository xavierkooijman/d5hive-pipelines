DESTINATIONS = {}


def register_destination(name):
    def wrapper(func):
        DESTINATIONS[name] = func
        return func
    return wrapper


def get_destination(name):
    if name not in DESTINATIONS:
        raise ValueError(f"Unknown destination: {name}")
    return DESTINATIONS[name]
