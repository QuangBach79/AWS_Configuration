import os
import json

def get_config_for_all_objects():
    subdirectory_path = '/var/task/config'
    file_name = 'data.json'
    file_path = os.path.join(subdirectory_path, file_name)
   
    try:
        with open(file_path, 'r') as file:
            config_data = json.loads(file.read())
        
        return config_data
            
    except FileNotFoundError:
        print("ERRO")
        raise
        
    