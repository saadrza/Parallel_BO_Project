import json
import math
import os
import pandas as pd
from omegaconf import OmegaConf

def calculate_score(data: dict) -> dict:
    """
    Function that calculates the score of a scenario based on the results of the LCA and LCC assessments. The function
    returns a dictionary with the cost and the impact indicators of the scenario.

    :param data: list of results of the LCA and LCC assessments
    :return: dictionary with the cost and the indicators impact of the scenario
    """
    result_dict = {'cost': {}, 'params': {}}

    for r in data:
        assessment_type = r.get("assessmentType")
        for phase_result in r['phaseResults']:
            for child in phase_result['nodeImpactResult']['impactList']:
                value = child['value']
                name = child['name']
                if assessment_type == "LCA":
                    result_dict['params'].setdefault(name, []).append(value)
                elif assessment_type == "LCC":
                    result_dict['cost'].setdefault(name, []).append(value)

    for k, v in result_dict['cost'].items():
        result_dict['cost'][k] = math.fsum(v)

    for k, v in result_dict['params'].items():
        result_dict['params'][k] = math.fsum(v)

    return result_dict


# Define the directory path from config or set it directly
config_path = 'config.yaml'
config = OmegaConf.load(config_path)
json_directory = config.get("json_dir", "./desktop/data/json")
#./json data\archive'
# List the contents of the directory

contents = os.listdir(json_directory)

# Initialize an empty DataFrame
df = pd.DataFrame()
columns = set()

# Loop through the contents
for item in contents:
    item_path = os.path.join(json_directory, item)
    if os.path.isdir(item_path):
        # List the contents of the subfolder
        folder_contents = os.listdir(item_path)
        count = 0

        # Loop through the contents of the subfolder
        for f in folder_contents:
            sub_item_path = os.path.join(item_path, f)
            if os.path.isfile(sub_item_path):
                # Open and read the JSON file
                with open(sub_item_path, 'r') as json_file:
                    try:
                        data = json.load(json_file)
                        if f == "customized_scenario.json":
                            count += 1
                            data = json.loads(data.replace("\\", ""))
                            parameter_dict = {}
                            for p in data["parameters"]:
                                parameter_dict[p["parameterName"]] = p["value"]
                            columns.update(parameter_dict.keys())
                            cs_temp_df = pd.DataFrame([parameter_dict])

                        elif f == "calculation.json":
                            count += 1
                            score = calculate_score(data)
                            flattened_data = {**score.get('cost', {}), **score.get('params', {})}
                            score_temp_df = pd.DataFrame([flattened_data])
                            columns.update(flattened_data.keys())
                    except json.JSONDecodeError as e:
                        print(f"Error reading JSON file: {e}")

        if count == 2:
            temp_df = pd.concat([cs_temp_df, score_temp_df], ignore_index=True, axis=1)
            df = pd.concat([df, temp_df], ignore_index=True, axis=0)

df.columns = columns
# Save the DataFrame to a pickle file and save using the config path
output_dir = config.get("dest_file", "./")
output_path = os.path.join(output_dir, "dataset11.pkl")
df.to_pickle(output_path)

