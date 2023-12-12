import json
import os
from util import extraction_config
import traceback
import datetime
import copy

MINIMUM_DELTA_TIME = int(os.getenv('MINIMUM_DELTA_TIME'))
DATETIME_FORMAT = os.getenv('DATETIME_FORMAT')

def lambda_handler(event, context):
    data_extraction_config = extraction_config.get_config_for_all_objects()
    
    the_end_time_of_current_window, minute_of_current_window = _find_the_time_of_current_window()
    
    data_extraction_config = _determine_objects_need_to_be_extracted_in_the_current_window(
        data_extraction_config, minute_of_current_window)
        
    batches = _configure_automatic_batches(data_extraction_config)
    
    batches = _determine_parent_children_objects(batches)
    
    return batches
        
    
    
    
def _find_the_time_of_current_window():
    try:
        current = datetime.datetime.now()
        new_minute = current.minute // MINIMUM_DELTA_TIME
        to_date = current.replace(minute=new_minute*MINIMUM_DELTA_TIME, second=0, microsecond=0)
        to_d = to_date.strftime(DATETIME_FORMAT)
        
        return to_d, to_date.minute

    except Exception as e:
        except_traceback = traceback.format_exc()
        print("ERRO: ", except_traceback)
        raise
    
def _determine_objects_need_to_be_extracted_in_the_current_window(
        data_extraction_config, minute_of_current_window):
    objects_need_to_be_extracted_in_the_current_window = \
        list(
            filter(
                lambda x: minute_of_current_window % x['config']['interval_time_to_extract'] == 0,
                data_extraction_config
            )
        )
    return objects_need_to_be_extracted_in_the_current_window
    
    
def _get_window_time(config):
    try:
        current = datetime.datetime.now()
        delta_time = config['config']['interval_time_to_extract']
        new_minute = current.minute // delta_time
        to_date = current.replace(minute=new_minute*delta_time, second=0, microsecond=0)
        from_date = to_date - datetime.timedelta(minutes=delta_time)
        to_d = to_date.strftime(DATETIME_FORMAT)
        from_d = from_date.strftime(DATETIME_FORMAT)

        return from_d, to_d
    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("ERRO: ", exception_traceback)
        raise
    
def _configure_automatic_batches(data_extraction_config):
    try:
        batches = []
        for config in data_extraction_config:
            from_d, to_d = _get_window_time(config)
            batches.append({
                "object": config['object'],
                "time_window":
                    {
                        "from": from_d,
                        "to": to_d
                    },  
                "config": config['config']
            })
            
        return list(batches)
    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("ERRO: ", exception_traceback)
        raise
    
def _determine_parent_children_objects(data_extraction_config):
    parent_children_pair ={}
    parent_objects_config = []
    for config in data_extraction_config:
        parent_name = config["config"]["parent"]
        if parent_name is not None:
            if parent_name in parent_children_pair:
                parent_children_pair[parent_name].append({
                    "object": config["object"],
                    "time_window": config["time_window"],
                    "config": config["config"]
                })
            else:
                parent_children_pair[parent_name] = [{
                    "object": config["object"],
                    "time_window": config["time_window"],
                    "config": config["config"]
                }]
        else:
            parent_objects_config.append(config)
    
    print(f'This is parent_children_pair: {parent_children_pair}')
    print(f'This is parent_objects: {parent_objects_config}')
    
    return generate_hierachy(parent_children_pair, parent_objects_config)
    
    
def generate_hierachy(parent_children_pair, parent_objects_config):
    hierachy=[]
    
    parent_child_dict = {}

    for parent, children in parent_children_pair.items():
        if parent not in parent_child_dict:
            parent_child_dict[parent] = []    
        parent_child_dict[parent].extend(children)

    for parent_obj in parent_objects_config:
        obj = copy.deepcopy(parent_obj)
        obj["children"] = build_children(obj["object"], parent_child_dict)
        hierachy.append(obj)

    return hierachy
    

def build_children(parent, parent_child_dict):
    children = []

    if parent in parent_child_dict:
        for child in parent_child_dict[parent]:
            child_obj = copy.deepcopy(child)
            child_obj["children"] = build_children(child_obj["object"], parent_child_dict)
            children.append(child_obj)

        return children
# def get_config_for_all_objects():
#     subdirectory_path = '/var/task/config'
#     file_name = 'data.json'
#     file_path = os.path.join(subdirectory_path, file_name)
   
#     try:
#         with open(file_path, 'r') as file:
#             data = json.loads(file.read())
            
#     except FileNotFoundError:
#         print("ERRO")
        
#     return data


