import json

def load_config(json_name: str):
    with open(f'stock_data_sink_configs/{json_name}', 'r') as config_file:
        return json.load(config_file)
