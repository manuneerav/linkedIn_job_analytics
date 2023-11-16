import pandas as pd
from sqlalchemy import create_engine
import pyodbc


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
#print(df_benefits.head())

#Transformations for job_skills table

job_skills_query = 'select * from job_skills'
df_job_skills = pd.read_sql(job_skills_query,connection)
df_job_skills.drop_duplicates(inplace=True)
df_job_skills['skill_abr'].fillna('Unknown',inplace=True)
#print(df_job_skills.head())

#Transformations for company_specialities table
company_specialities_query = 'select * from company_specialities'
df_company_specialities = pd.read_sql(company_specialities_query,connection)
df_company_specialities.drop_duplicates(inplace=True)
df_company_specialities['speciality'].fillna('Unknown',inplace=True)
#print(df_company_specialities.head())

#Transformations for company_industries table
company_industries_query = 'select * from company_industries'
df_company_industries = pd.read_sql(company_industries_query,connection)
df_company_industries.drop_duplicates(inplace=True)
df_company_industries['industry'].fillna('Unknown',inplace=True)
#print(df_company_industries.head())

#Transformations for employee_counts table
count_list = []
employee_counts_query = 'select * from employee_counts'
df_employee_counts = pd.read_sql(employee_counts_query,connection)
df_employee_counts.drop_duplicates(inplace=True)
df_employee_counts['time_recorded'] = pd.to_datetime(df_employee_counts['time_recorded']).dt.date
count_list = df_employee_counts[df_employee_counts['follower_count'].isnull()]['company_id']
for i in count_list:
    df_employee_counts.loc[df_employee_counts['company_id'] == i,'follower_count'] = df_employee_counts[df_employee_counts['company_id'] == i]['follower_count'].mean()
#print(df_employee_counts.head())

#Transformations for companies table
companies_query = 'select * from companies'
df_companies = pd.read_sql(companies_query,connection)
#df_companies.drop(df_companies[df_companies['name'].isnull().index])
df_companies_new = df_companies.dropna(subset=['name'])

replacement_value = {col:'Not Provided' for col in df_companies_new.columns}
df_companies_new.fillna(replacement_value,inplace=True)
print(df_companies_new.head())
print(df_companies_new['country'].head())
print(df_companies_new['state'].head())

#Transformations for job_postings table
job_postings_query = 'select * from job_postings'
df_job_postings = pd.read_sql(job_postings_query,connection)
#df_job_postings.drop(df_job_postings[df_job_postings['company_id'].isnull().index()])

#need to drop tables-posting_domain, skills_desc, closed_time, application_url, job_posting_url, original_listed_time, med salary
col_to_drop = ['posting_domain', 'skills_desc', 'closed_time', 'application_url', 'job_posting_url', 'original_listed_time', 'med_salary']
df_job_postings_new = df_job_postings.drop(col_to_drop,axis=1)
#print(df_job_postings_new.shape)
ndf = df_job_postings_new.dropna(subset=['company_id'])
ndf['title'].fillna('Not Provided',inplace=True)
ndf['description'].fillna('Not Provided',inplace=True)
ndf['pay_period'].fillna('Not Provided',inplace=True)
ndf['work_type'].fillna('Not Provided',inplace=True)
ndf['location'].fillna('Not Provided',inplace=True)
ndf['applies'].fillna(0,inplace=True)
ndf['remote_allowed'].fillna(0,inplace=True)
ndf['views'].fillna(0,inplace=True)
ndf['application_type'].fillna('Not Provided',inplace=True)
ndf['formatted_experience_level'].fillna('Not Provided',inplace=True)
ndf['sponsored'].fillna(0,inplace=True)
ndf['currency'].fillna('Unknown',inplace=True)
ndf['compensation_type'].fillna('Unknown',inplace=True)


# Finding the company_id which have null value in max_salary column
# replacing null value with the mean of max_salary.(mean calculated for specific company_id)
count_list1 = []
count_list1 = ndf[ndf['max_salary'].isnull()]['company_id']
for i in count_list1:
    ndf.loc[ndf['company_id'] == i,'max_salary'] = ndf[ndf['company_id'] == i]['max_salary'].mean()

# Finding the company_id which have null value in min_salary column
# replacing null value with the mean of min_salary.(mean calculated for specific company_id)
count_list2 = []
count_list2 = ndf[ndf['min_salary'].isnull()]['company_id']
for j in count_list2:
    ndf.loc[df_job_postings['company_id'] == i,'min_salary'] = ndf[ndf['company_id'] == i]['min_salary'].mean()

print(df_job_postings['max_salary'].head())
#print(ndf.shape)
#print(ndf.head())



