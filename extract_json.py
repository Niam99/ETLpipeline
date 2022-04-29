import pandas as pd
import json

jsonfile = "10383.json"
def read_json(filename: str) -> dict:
    try:
        with open(filename, "r") as f:
            data = json.loads(f.read())
    except:
            raise Exception(f"Reading {filename} file encountered an error")

    return data
test2 = ["11380.json","11381.json","11382.json"]



def collect_data(list_of_files):
    data = pd.DataFrame()
    for item in list_of_files:
        d = read_json(item)
        df=pd.Series(d)
        data=data.append(df, ignore_index=True)
    data.to_csv("json_all.csv", encoding='utf-8', index=False)
    return data

print(collect_data(test2))





# Implementing boto3
# s3_client = boto3.client('s3')
#
# bucket_list = s3_client.list_buckets()
# bucket_name = 'data-28-final-project-files-group2'
# folder_name = 'Talent'

# bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix= folder_name) #gets all contents from Talent file
# #pp.pprint(bucket_contents)
#
# for element in bucket_contents['Contents']:
#     print(element['Key'])











# jsonfile = "10383.json    "
# file_name = jsonfile.split('.')[0] #gets id number from

#test = read_json(jsonfile)
# def convert_to_csv(file):
#     data = pd.DataFrame.from_dict(file,orient='index').T
#     #data.to_csv(f"{file}.csv", encoding='utf-8', index=False)
#     data.to_csv(f"{file_name}.csv", encoding='utf-8', index=False)



# convert_to_csv(test)

# data = pd.DataFrame([])
# def convert(file):
#     for i in file:
#         x = pd.Series.from_dict(i,orient='index').T
#         data.append(x)
#     data.to_csv("json_all.csv", encoding='utf-8', index=False)