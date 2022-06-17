from datetime import datetime
import os
import json
import re

from pendulum import date

# dt = datetime.now()
# ts = datetime.timestamp(dt)
# print("Date and time is:", dt)
# print("Timestamp is:", ts)

# example_unix = 1655018812
# example_conversion = datetime.fromtimestamp(example_unix)
# example_conversion_string = example_conversion.strftime('%Y-%m-%d-%H-%M-%S')
# print(example_conversion)
# print(example_conversion_string)
# print(type(example_conversion))
# testing1 = example_conversion.strftime("%Y-%m-%d-%H%M%S")
# print(testing1)
# print(type(example_conversion))
# print(example_conversion.strftime('%Y-%m'))
# print(f"This is an example conversion {example_conversion}")


# for file in os.listdir(r'C:\Users\Matt\Desktop\satellite_tracker\satellite_tracker\docker\data'):
#     print(file)

# file_list = [file for file in os.listdir(r'C:\Users\Matt\Desktop\satellite_tracker\satellite_tracker\docker\data')]
# homepath = r'C:\Users\Matt\Desktop\satellite_tracker\satellite_tracker\docker\data'
     
            
       
# def merge_jsonfiles(file_list: list):
#     combined_json = []
#     for file in file_list:
#         with open(f'{homepath}\{file}','r') as infile:
#             file_contents = json.load(infile)
#             combined_json.append(file_contents)
#     with open(fr'C:\Users\Matt\Desktop\satellite_tracker\satellite_tracker\docker\data\this_is_a_test.json','w') as output_file:
#         json.dump(combined_json,output_file,indent=2)

# merge_jsonfiles(file_list)


test = 'position-2022-06-12-21-05-58.json'
pattern = re.compile(r"position-|tle-|.json")
earliest_time = pattern.split(test)[1]
print(earliest_time)
print(type(earliest_time))

# test_split = ''.join(test.split('position- | .json'))
# print(test_split)