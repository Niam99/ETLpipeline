import pandas as pd
import json
import boto3

s3_client = boto3.client('s3')
bucket_list = s3_client.list_buckets()
bucket_name = 'data-28-final-project-files-group2'
bucket_contents = s3_client.list_objects_v2(Bucket='data-28-final-project-files-group2', Prefix='Talent')["Contents"]
paginator = s3_client.get_paginator('list_objects_v2')

op_param = {'Bucket': 'data-28-final-project-files-group2',
            'Prefix': 'Talent'}
page_iter = paginator.paginate(**op_param)




def extract_data():
    combined_data = pd.DataFrame()
    for page in page_iter:
        for content in page['Contents']:
            if content['Key'].endswith('.json'):
                result_json = content['Key']
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=result_json)
                data = json.load(s3_object['Body'])  # returns contents of file as dictionary
                data_df = pd.DataFrame.from_dict(data, orient='index').T  # from dictionary converts to dataframe
                combined_data = pd.concat([combined_data, data_df], axis=0)  # adds data onto the dataframe
                combined_data.to_csv("final_json_all.csv", encoding='utf-8', index=False)  # converts data to csv
    return combined_data


a=extract_data()
print(a.shape)

