import pandas as pd
import pyodbc
import os
from google.cloud import bigquery

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/neera/OneDrive/Desktop/etl-demo-project-385707-17dcdf5a87ca.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/neera/Downloads/etl-demo-project-385707-b4d5f241a060.json"

#def linkedin_etl():
#create_engine('mssql+pyodbc://server_name/database_name?driver?trusted_connection=yes')
connection = pyodbc.connect(
    Trusted_Connection='Yes',
    Driver='{SQL Server}',
    server='NEERAV-PC\SQLEXPRESS',
    database='LinkedIn_Jobs'
    
)
cursor = connection.cursor()

#Transformations for benefits table

benefits_query = 'select * from benefits'
df_benefits = pd.read_sql(benefits_query,connection)
df_benefits.drop_duplicates(inplace=True)
df_benefits['type'].fillna(df_benefits['type'].mode(),inplace=True)
print(df_benefits.head())

#Transformations for job_skills table

job_skills_query = 'select * from job_skills'
df_job_skills = pd.read_sql(job_skills_query,connection)
df_job_skills.drop_duplicates(inplace=True)
df_job_skills['skill_abr'].fillna('Unknown',inplace=True)
print(df_job_skills.head())

#Transformations for company_specialities table
company_specialities_query = 'select * from company_specialities'
df_company_specialities = pd.read_sql(company_specialities_query,connection)
df_company_specialities.drop_duplicates(inplace=True)
df_company_specialities['speciality'].fillna('Unknown',inplace=True)
print(df_company_specialities.head())

#Transformations for company_industries table
company_industries_query = 'select * from company_industries'
df_company_industries = pd.read_sql(company_industries_query,connection)
df_company_industries.drop_duplicates(inplace=True)
df_company_industries['industry'].fillna('Unknown',inplace=True)
print(df_company_industries.head())

#Transformations for employee_counts table
count_list = []
employee_counts_query = 'select * from employee_counts'
df_employee_counts = pd.read_sql(employee_counts_query,connection)
df_employee_counts.drop_duplicates(inplace=True)
df_employee_counts['time_recorded'] = pd.to_datetime(df_employee_counts['time_recorded']).dt.date
count_list = df_employee_counts[df_employee_counts['follower_count'].isnull()]['company_id']
for i in count_list:
    df_employee_counts.loc[df_employee_counts['company_id'] == i,'follower_count'] = df_employee_counts[df_employee_counts['company_id'] == i]['follower_count'].mean()
print(df_employee_counts.head())


#Transformations for job_postings table
job_postings_query = 'select * from job_postings'
df_job_postings = pd.read_sql(job_postings_query,connection)
#df_job_postings.drop(df_job_postings[df_job_postings['company_id'].isnull().index()])

#need to drop tables-posting_domain, skills_desc, closed_time, application_url, job_posting_url, original_listed_time, med salary
col_to_drop = ['posting_domain', 'skills_desc', 'closed_time', 'application_url', 'job_posting_url', 'original_listed_time', 'med_salary']
df_job_postings = df_job_postings.drop(col_to_drop,axis=1)
print(df_job_postings.shape)
df_job_postings.dropna(subset=['company_id'])
df_job_postings['expiry'] = pd.to_datetime(df_job_postings['expiry']).dt.date
df_job_postings['listed_time'] = pd.to_datetime(df_job_postings['listed_time']).dt.date
df_job_postings['title'].fillna('Not Provided',inplace=True)
df_job_postings['description'].fillna('Not Provided',inplace=True)
df_job_postings['pay_period'].fillna('Not Provided',inplace=True)
df_job_postings['work_type'].fillna('Not Provided',inplace=True)
df_job_postings['location'].fillna('Not Provided',inplace=True)
df_job_postings['applies'].fillna(0,inplace=True)
df_job_postings['remote_allowed'].fillna(0,inplace=True)
df_job_postings['views'].fillna(0,inplace=True)
df_job_postings['application_type'].fillna('Not Provided',inplace=True)
df_job_postings['formatted_experience_level'].fillna('Not Provided',inplace=True)
df_job_postings['sponsored'].fillna(0,inplace=True)
df_job_postings['currency'].fillna('Unknown',inplace=True)
df_job_postings['compensation_type'].fillna('Unknown',inplace=True)


# Finding the company_id which have null value in max_salary column
# replacing null value with the mean of max_salary.(mean calculated for specific company_id)
count_list1 = []
count_list1 = df_job_postings[df_job_postings['max_salary'].isnull()]['company_id']
for i in count_list1:
    df_job_postings.loc[df_job_postings['company_id'] == i,'max_salary'] = df_job_postings[df_job_postings['company_id'] == i]['max_salary'].mean()

# Finding the company_id which have null value in min_salary column
# replacing null value with the mean of min_salary.(mean calculated for specific company_id)
count_list2 = []
count_list2 = df_job_postings[df_job_postings['min_salary'].isnull()]['company_id']
for j in count_list2:
    df_job_postings.loc[df_job_postings['company_id'] == i,'min_salary'] = df_job_postings[df_job_postings['company_id'] == i]['min_salary'].mean()

#print(df_job_postings.head())
#print(df_job_postings.head())

#Transformations for companies table
companies_query = 'select * from companies'
df_companies = pd.read_sql(companies_query,connection)
#df_companies.drop(df_companies[df_companies['name'].isnull().index])
df_companies = df_companies.dropna(subset=['name'])
#replacement_value = {col:'Unknown' for col in df_companies.columns}
#df_companies.fillna(replacement_value,inplace=True)
#df_companies['company_size'] = df_companies['company_size'].astype(int)
df_companies['company_size'].fillna(0,inplace=True)
df_companies['description'].fillna('Unknown',inplace=True)
df_companies['state'].fillna('Unknown',inplace=True)
df_companies['country'].fillna('Unknown',inplace=True)
df_companies['city'].fillna('Unknown',inplace=True)
df_companies['zip_code'].fillna('Unknown',inplace=True)
df_companies['address'].fillna('Unknown',inplace=True)

print(df_companies.head())

df_benefits.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.benefits',
if_exists = 'append',
chunksize = 10000

)

df_job_skills.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.job_skills',
if_exists = 'append',
chunksize = 10000

)

df_company_specialities.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.company_specialities',
if_exists = 'append',
chunksize = 10000

)

df_company_industries.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.company_industries',
if_exists = 'append',
chunksize = 10000

)

df_employee_counts.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.employee_counts',
if_exists = 'append',
chunksize = 10000,
table_schema=[
    {'name': 'time_recorded', 'type': 'DATE'}
]

)

df_companies.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.companies',
if_exists = 'append',
chunksize = 10000,

)

df_job_postings.to_gbq(
    
project_id = 'etl-demo-project-385707',
destination_table = '01etl.job_postings',
if_exists = 'append',
chunksize = 10000,
table_schema=[
    {'name': 'expiry', 'type': 'DATE'},
    {'name': 'listed_time', 'type': 'DATE'},
    {'name': 'applies', 'type': 'INTEGER'},
    {'name': 'views', 'type': 'INTEGER'},
    {'name': 'remote_allowed', 'type': 'BOOLEAN'}

]

)


print(df_benefits.head())




