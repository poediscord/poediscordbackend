import json
import os

config = None

INSTANCE = "./instance"

def load_config(testing=False):
    global config
    config_name = (not testing) and "config.json" or "config_testing.json"
    config_file = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", INSTANCE, config_name))
    with open(config_file) as file:
        config = json.load(file)
    return config