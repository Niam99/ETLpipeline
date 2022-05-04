import json
import re
import boto3
from pprint import pprint as pp
import pandas as pd
import csv
from csv import writer as wt
import os.path
from glob import glob
from io import StringIO
import datetime

s3_client = boto3.client('s3')

bucket_list = s3_client.list_buckets()
bucket_name = 'data-28-final-project-files-group2'

# =====================================================================
folder = "Data28group2cleanfiles"

name_student_and_trainers = 'All_students_and_trainers'
txtfiles = 'Alltxt_to_csv'
applicants_filename = 'Applicantcsvs'


# ====================================================================================

def get_course_date(i):
    filename = i['Key']
    name = filename.split('/')[1].split('_')
    course = '_'.join([name[0], name[1]])
    date = name[2].split('.')[0]
    #     print(course)
    return course, date


def get_csvs(client, bucket_name, pref):
    """ Takes in a client and bucket name concatenate all the csvs inside
    and return a pandas dataframe
    to update :: with an extra column specifying the course title and class
    name tutor score score ..... course date
    """
    """
    Given a filename returns the contents of the csv file aws a df
    is to be used as a function while looping through all the files
    """
    bucket_contents = client.list_objects(Bucket=bucket_name, Prefix=pref)["Contents"]

    academic = pd.DataFrame()

    for i in bucket_contents:
        course_date = get_course_date(i)
        latest_course = client.get_object(Bucket=bucket_name, Key=i["Key"])
        a = pd.read_csv(latest_course["Body"])
        a['course'] = course_date[0]

        a['start_date'] = course_date[1]

        academic = pd.concat([academic, a])

    return academic


def reind(df):
    """
    To rearrange the columns
    """
    col = list(df.columns)
    col.remove('course')
    col.remove('start_date')
    col.extend(['course', 'start_date'])
    df = df.reindex(columns=col)
    df['old index'] = df.index
    df = df.reset_index(drop=True)
    return df


def get_csvs_all_courses(client, bucket_name, one_file=True):
    """ Takes in a client and bucket name concatenate all the csvs inside
        and return a pandas dataframe
        to update :: with an extra column specifying the course title and class
        name tutor score score ..... course date
        """

    """
    Puts it all together and return one dataframe if one_file is Truee (joins all the differnet courses in one dataframe)
        returns a list of three data frames ( one for each course) iff one_file is False.
        """
    prefixes = ['Academy/Business', 'Academy/Data', 'Academy/Engineering']
    full_list = pd.DataFrame()
    files = []
    for pref in prefixes:
        b = get_csvs(client, bucket_name, pref)
        b = reind(b)

        if one_file:
            full_list = pd.concat([full_list, b])
        else:

            files.append(b)
        print(pref, 'is done')
    print('done')
    return full_list if one_file else files


def upload(client=s3_client, bucket_name=bucket_name, one_file=True, folder=folder, name=name_student_and_trainers):
    """
    Uploads this code.
    uploads onefile if one_file is true other wise upload 3 files(Business,Data,Engineering)
    upload()
    """

    if one_file:

        key = f"{folder}/{name}.csv"
        dff = get_csvs_all_courses(s3_client, bucket_name, one_file=one_file)
        s3_client.put_object(Bucket=bucket_name, Body=dff.to_csv(), Key=key)

    else:
        #   bucket_name,filename,foolder,dum
        subjects = get_csvs_all_courses(s3_client, bucket_name, one_file=False)
        filenames = ['Business.csv', 'Data.csv', 'Engineering.csv']
        for i in range(len(filenames)):
            key = f"{folder}/{filenames[i]}"

            print('uploading ', filenames[i])

            content = subjects[i].to_csv()
            #         subjects[i].to_csv(filenames[i])
            client.put_object(Bucket=bucket_name, Body=subjects[i].to_csv(), Key=key)
    return 'done'


# =======================================================================
def get_bucket_cont_business():
    contents = s3_client.list_objects(Bucket=bucket_name, Prefix="Data28group2cleanfiles1/Business")["Contents"]
    return contents


def get_bucket_cont_data():
    cont_data = s3_client.list_objects(Bucket=bucket_name, Prefix="Data28group2cleanfiles1/Data")["Contents"]
    return cont_data


def get_bucket_cont_engineering():
    cont_eng = s3_client.list_objects(Bucket=bucket_name, Prefix="Data28group2cleanfiles1/Engineering")["Contents"]
    return cont_eng


def get_body_business():
    for contents in get_bucket_cont_business():
        key = s3_client.get_object(Bucket=bucket_name, Key=contents["Key"])
        body = pd.read_csv(key["Body"])
        return body


def get_body_data():
    for contents in get_bucket_cont_data():
        data_key = s3_client.get_object(Bucket=bucket_name, Key=contents["Key"])
        data_body = pd.read_csv(data_key["Body"])
        return data_body


def get_body_engineering():
    for contents in get_bucket_cont_engineering():
        eng_key = s3_client.get_object(Bucket=bucket_name, Key=contents["Key"])
        eng_body = pd.read_csv(eng_key["Body"])
        return eng_body


def concat_all3files():
    concat3 = pd.DataFrame()
    concat3 = pd.concat([concat3, get_body_business(), get_body_data(), get_body_engineering()])
    return concat3


# pp(concat_all3files())
def send_all_csv_students_to_s3():
    s3_client.put_object(Bucket=bucket_name, Body=concat_all3files().to_csv(),
                         Key='Data28group2cleanfiles/All3Courses.csv')


# ====================Compiling the text files===========================

def txt_to_df(s3_client, bucket_name, a):
    txtfile = s3_client.get_object(Bucket=bucket_name, Key=a["Key"])
    txt = txtfile['Body'].read()
    txt = txt.decode('utf-8')
    txt = txt.replace('\r', '')
    txt = txt.replace('Psychometrics: ', ',')
    txt = txt.replace('Presentation: ', '').strip()
    txt_list = txt.split('\n')
    inter_day = txt_list[0]
    inter_uni = txt_list[1]
    txt_fi = txt_list[3:]

    col = ['name', 'Psychometrics', 'Presentation', 'Date of test', 'Academy']
    inter_day = datetime.datetime.strptime(inter_day, '%A %d %B %Y')
    new_txt_fi = []
    for i in txt_fi:
        b = i.split(',')
        b[0] = b[0].strip(' ').strip('-').strip(' ')
        new_txt_fi.append(','.join(b))
    new_txt_fi_2 = []
    for i in new_txt_fi:
        dummy = i.split(',')
        dummy.append(str(inter_day))  # i changed this to string and added an import datetime at the top
        dummy.append(inter_uni)
        new_txt_fi_2.append(dummy)

    txt_df = pd.DataFrame(new_txt_fi_2)
    txt_df.columns = col
    del txt
    del txt_list
    del txt_fi
    del new_txt_fi
    return txt_df


def upload_alltxt_as_csv(s3_client=s3_client, bucket_name=bucket_name, key=f'{folder}/{txtfiles}.csv'):
    bucket_cont = s3_client.list_objects(Bucket=bucket_name, Prefix="Talent/Sparta Day")["Contents"]
    all_df = pd.DataFrame()
    for a in bucket_cont:
        dummy = txt_to_df(s3_client, bucket_name, a)
        all_df = pd.concat([all_df, dummy])
    all_df['old index'] = all_df.index
    all_df = all_df.reset_index(drop=True)
    s3_client.put_object(Bucket=bucket_name, Body=all_df.to_csv(), Key=key)
    return 'done'


# ===============Functions to upload The applicants data=======================================

def get_all_files_folder(s3_client, bucket_name, folder):
    """ This function gets all the files from a folder"""
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=bucket_name, Prefix=folder, PaginationConfig={"MaxItems": 6000})
    fij = []
    for page in response:
        #     print("getting 2 files from S3")
        files = page.get("Contents")
        for file in files:
            #         print(f"file_name: {file['Key']}, size: {file['Size']}")
            fij.append(file)
    return fij


def Application_csv(s3_client, bucket_name):
    #     loops through the pages and get all files filter them and get all csv as a dataframe
    def filter_applicants(x):
        if x['Key'].endswith('Applicants.csv'):
            return True
        return False

    def clean_degree(x):
        x = str(x)
        c = x.strip().split(':')
        if len(c) > 1:
            c = '.'.join(c)
        else:
            c = c[0][0]
        return (c)

    def clean(x):
        x = str(x)
        x = re.sub('-|\(|\)|\s', '', x)
        return str(x)

    key = 'Talent'
    bucket_content_name = get_all_files_folder(s3_client, bucket_name, 'Talent')
    bucket_content_name = [i for i in bucket_content_name if filter_applicants(i)]
    applications = pd.DataFrame()

    for i in bucket_content_name:
        monthly_application = s3_client.get_object(Bucket=bucket_name, Key=i["Key"])
        a = pd.read_csv(monthly_application["Body"])
        a['Filename'] = i['Key'].split('/')[1].split('.')[0]  # new column

        applications = pd.concat([applications, a])

    applications['degree'] = applications['degree'].apply(clean_degree)
    applications['phone_number'] = applications['phone_number'].apply(clean)
    return applications


def upload_applicants(filename=f'{folder}/{applicants_filename}.csv'):
    # upload the df gotten from applicants to a particular filename
    df = Application_csv(s3_client, bucket_name)
    s3_client.put_object(Bucket=bucket_name, Body=df.to_csv(), Key=filename)
    return 'done'


# ------------ json extraction -------------------------------------------------------------------

s3_client = boto3.client('s3')
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
                data_df['id'] = content['Key'].split('/')[1].split('.')[0]  # adds if from the file name
                combined_data = pd.concat([combined_data, data_df], axis=0)  # adds data onto the dataframe
                combined_data.to_csv("final_json_all.csv", encoding='utf-8', index=False)  # converts data to csv
    return combined_data

a = extract_data()

def upload_to_s3(filename,key, bucketname = bucket_name,):
    s3_client.upload_file(Filename= filename, Bucket=bucket_name,Key=key)

upload_to_s3(filename='final_json_all.csv',key='Data28group2cleanfiles/FinalJsonFile.csv')