from airflow.operators.email_operator import EmailOperator
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


#Please mention the path to these files here
sqlJar="mysql-connector-java-5.1.49.jar"
scriptPath="Sql2Csv.py"
scriptPath2="csv2Udf2Csv.py"

#Assuming the database to be named test and table temp
database="test"
table="temp"
#please enter password here
password=""
user="root"

#PLease enter both the output paths here
outputFolder=""
outputFolder2=""
Sql2CsvCmd="spark-submit  --jars " + sqlJar + " " + scriptPath + " " + database + " " + table + " " + password + " " + user + " " +outputFolder

csv2Udf2CsvCmd = "spark-submit  " + scriptPath2 + " " + outputFolder + " " + outputFolder2




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'GanitAssignmentDag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=None,
)


emailConfirmation = EmailOperator(
    task_id="emailConfirmation", 
    to='anmol.jatin78@gmail.com',
    subject='Success Email',
    html_content='<p> The Dag has completed successfully <p>',
    dag=dag)



Sql2Csv = BashOperator(task_id='Sql2Csv',
					   bash_command=Sql2CsvCmd,
					   dag=dag)

csv2Udf2Csv = BashOperator(task_id='csv2Udf2Csv',
                                           bash_command=csv2Udf2CsvCmd,
                                           dag=dag)




Sql2Csv >> csv2Udf2Csv >> emailConfirmation
