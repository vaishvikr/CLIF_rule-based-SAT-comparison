import json
import os

def load_config():
    json_path = os.path.join("config", "config.json")
    
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            config = json.load(file)
        print("Loaded configuration from config.json")
    else:
        raise FileNotFoundError("Configuration file not found.",
                                "Please create config.json based on the config_template.")
    
    return config
# Load the configuration
config = load_config()
