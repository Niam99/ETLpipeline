import pandas as pd
import pyodbc
import boto3
from pprint import pprint

# s3_client = boto3.client('s3')
# bucket_list = s3_client.list_buckets()
# bucket_name = 'data-28-final-project-files-group2'
# s3_object = s3_client.get_object(Bucket=bucket_name, Key="Data28group2cleanfiles1/SpartaTestDay.csv")
# data = pd.read_csv(s3_object["Body"])
# df = pd.DataFrame(data)
# df.drop(df.columns[0], axis=1, inplace=True)
# df.drop(df.columns[-1], axis=1, inplace=True)
# df = df.rename(columns={"Date of test": "date_of_test"})
# print(df)
'''
server = '34.89.1.218'
database = 'sparta'
username = 'root'
password = ''
'''

# for index, row in df.iterrows():
#     cursor.execute("INSERT INTO talents (trainee_name,psychometrics_score,presentation_score, test_date,academy) values(?,?,?,?,?)", row.name, row.Psychometrics, row.Presentation, row.date_of_test, row.Academy)

# for index, row in df.iterrows():
#     cursor.execute("INSERT INTO trainees (trainee_name,psychometrics_score,presentation_score, test_date,academy) values(?,?,?,?,?)", row.name, row.Psychometrics, row.Presentation, row.date_of_test, row.Academy)
s3_client = boto3.client('s3')
bucket_list = s3_client.list_buckets()
bucket_name = 'data-28-final-project-files-group2'

docker_sparta = pyodbc.connect(
    'DRIVER={MySQL ODBC 8.0 ANSI Driver};User=root;Password=seb;Database=sparta;Server=34.89.1.218;Port=3306;')
cursor = docker_sparta.cursor()


def insert_applicants():
    s3_object = s3_client.get_object(Bucket=bucket_name, Key="Data28group2cleanfiles/Applicants.csv")
    data = pd.read_csv(s3_object["Body"])
    df = pd.DataFrame(data)
    df.drop_duplicates(subset="name", keep='last', inplace=True)
    pprint(df)

    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO applicants (trainee_name,gender,dob,email,city,address,postcode,phone_number,uni,degree,invite_date,applicant_month,invited_by) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row.name, row.gender, row.dob, row.email, row.city, row.address, row.postcode, row.phone_number, row.uni,
            row.degree, row.invited_date, row.month, row.invited_by)
    docker_sparta.commit()
    print(cursor.rowcount, "Record inserted successfully into table")
    cursor.close()


insert_applicants()


def insert_spartatestday():
    s3_object = s3_client.get_object(Bucket=bucket_name, Key="Data28group2cleanfiles/SpartaTestDay.csv")
    data = pd.read_csv(s3_object["Body"])
    df = pd.DataFrame(data)
    df = df.rename(columns={"Date of test": "date_of_test"})
    df = df.rename(columns={"Unnamed: 0": "ID"})
    pprint(df)

    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO spartatestday (ID,trainee_name,psychometrics_score,presentation_score,test_date,academy) values(?,?,?,?,?,?)",
            row.ID, row.name, row.Psychometrics, row.Presentation, row.date_of_test, row.Academy)


# insert_spartatestday()

def insert_talent():
    s3_object = s3_client.get_object(Bucket=bucket_name, Key="Data28group2cleanfiles/Talent.csv")
    data = pd.read_csv(s3_object["Body"])
    df = pd.DataFrame(data)
    pprint(df)

    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO spartatestday (trainee_name,talent_date,tech_self_score,strengths,weakness,self_dev,geo,financial,result,course_interest) values(?,?,?,?,?)",
            row.name, row.Psychometrics, row.Presentation, row.date_of_test, row.Academy)
    docker_sparta.commit()
    print(cursor.rowcount, "Record inserted successfully into Laptop table")
    cursor.close()


def insert_trainees():
    s3_object = s3_client.get_object(Bucket=bucket_name, Key="Data28group2cleanfiles/trainees.csv")
    data = pd.read_csv(s3_object["Body"])
    df = pd.DataFrame(data)
    pprint(df)

    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO trainees (trainee_name,trainer,analytic_w1,independent_w1,determined_w1,professional_w1,studious_w1,imaginative_w1,analytic_w2,independent_w2,determined_w2,professional_w2,studious_w2,imaginative_w2,analytic_w3,independent_w3,determined_w3,professional_w3,studious_w3,imaginative_w3,analytic_w4,independent_w4,determined_w4,professional_w4,studious_w4,imaginative_w4,analytic_w5,independent_w5,determined_w5,professional_w5,studious_w5,imaginative_w5,analytic_w6,independent_w6,determined_w6,professional_w6,studious_w6,imaginative_w6,analytic_w7,independent_w7,determined_w7,professional_w7,studious_w7,imaginative_w7,analytic_w8,independent_w8,determined_w8,professional_w8,studious_w8,imaginative_w8,analytic_w9,independent_w9,determined_w9,professional_w9,studious_w9,imaginative_w9,analytic_w10,independent_w10,determined_w10,professional_w10,studious_w10,imaginative_w10,course_name,course_start_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row.name, row.trainer, row.Analytic_W1, row.Independent_W1, row.Determined_W1, row.Professional_W1,
            row.Studious_W1, row.Imaginative_W1, row.Analytic_W2, row.Independent_W2, row.Determined_W2,
            row.Professional_W2, row.Studious_W2, row.Imaginative_W2, row.Analytic_W3, row.Independent_W3,
            row.Determined_W3, row.Professional_W3, row.Studious_W3, row.Imaginative_W3, row.Analytic_W4,
            row.Independent_W4, row.Determined_W4, row.Professional_W4, row.Studious_W4, row.Imaginative_W4,
            row.Analytic_W5, row.Independent_W5, row.Determined_W5, row.Professional_W5, row.Studious_W5,
            row.Imaginative_W5, row.Analytic_W6, row.Independent_W6, row.Determined_W6, row.Professional_W6,
            row.Studious_W6, row.Imaginative_W6, row.Analytic_W7, row.Independent_W7, row.Determined_W7,
            row.Professional_W7, row.Studious_W7, row.Imaginative_W7, row.Analytic_W8, row.Independent_W8,
            row.Determined_W8, row.Professional_W8, row.Studious_W8, row.Imaginative_W8, row.Analytic_W9,
            row.Independent_W9, row.Determined_W9, row.Professional_W9, row.Studious_W9, row.Imaginative_W9,
            row.Analytic_W10, row.Independent_W10, row.Determined_W10, row.Professional_W10, row.Studious_W10,
            row.Imaginative_W10, row.course, row.start_date)

# insert_trainees()
# docker_sparta.commit()
# cursor.close()

# def print_all_product_records():
#     query_records = _sql_query("SELECT * FROM spartatestday")
#     while True:
#         record = query_records.fetchone()
#         if record is None:
#             break
#         print(record)
#
# print_all_product_records()
