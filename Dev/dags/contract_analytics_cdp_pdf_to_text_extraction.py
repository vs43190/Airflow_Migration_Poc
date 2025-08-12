import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from Cluster_Configuration_Utility import start_cluster, stop_cluster
from airflow.operators.python_operator import PythonOperator

# Grouping all variables together at the top for better maintainability
no_of_retries = 0                          # Number of retries before failing a task.
retry_delay = 5                              # Time delay between retries (in seconds).
execution_timeout = 172800                  # Maximum execution time for the DAG.
end_date = '9999-12-31'                                    # End date for the DAG.
dag_creation_date = '2025-06-25'                  # DAG creation date.
dag_name = 'contract_analytics_cdp_pdf_to_text_extraction'                                  # Name of the batch/job.
schedule = '0 10 * * 1'                                    # Cron expression or schedule interval for DAG runs.
dag_tag = 'contract_analytics_cdp_prod_airflow'                                      # Tags for organizing DAGs.
dag_owner = 'Sunita.Badola@Takeda.com'                                  # Owner of the DAG.
dag_owner_email = 'Sunita.Badola@Takeda.com'                          # Email of the DAG owner.
sla_time = 60                                      # SLA time for task completion.
databricks_connection_id = 'MWAA_DBX_CONNECTION'    # Databricks connection ID.
MWAA_ENV_NAME = Variable.get('MWAA_ENV_NAME')
MWAA_IAM_ROLE = Variable.get('MWAA_IAM_ROLE')
MWAA_REGION = Variable.get('MWAA_REGION')
SNS_TOPIC_ARN = Variable.get('SNS_TOPIC_ARN')
batch_start_control_params = eval('''{
                                "notebook_path": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/start_batch_control",
                                "base_parameters": {"batch_name": "contract_analytics_cdp_pdf_to_text_extraction", "user_id": "varun.potlacheurvu@takeda.com", "mwaa_region": MWAA_REGION, "mwaa_env_name": MWAA_ENV_NAME, "mwaa_iam_role": MWAA_IAM_ROLE},
                                "source": "WorkSpace"
                            }''')  # Parameters for batch start control.
batch_stop_control_params = {'notebook_path': '/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/stop_batch_control', 'base_parameters': {'batch_name': 'contract_analytics_cdp_pdf_to_text_extraction', 'user_id': 'varun.potlacheurvu@takeda.com'}, 'source': 'WorkSpace'}    # Parameters for batch stop control.

cluster_id = Variable.get('cluster_mwaa_cdp_contract_analytics_dag')

cluster_config = {
  "cluster_name": "DE-CONTRACT-CLASSIFICATION-UC",
  "spark_version": "15.4.x-gpu-ml-scala2.12",
  "node_type_id": "g4dn.xlarge",
  "autotermination_minutes": 10,
  "policy_id": "00061EE663A57149",

  "clusowner": {
    "email": "sunita.badola@takeda.com",
    "project-id": "APMS-10883",
    "account-type": "prod"
  },

  "autoscale": {
    "min_workers": 1,
    "max_workers": 5
  },

  "spark_conf": {
    "spark.databricks.hive.metastore.glueCatalog.enabled": "true"
  },

  "spark_env_vars": {
    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
  },

  "cluster_log_conf": {
    "s3": {
      "destination": "s3://tpc-aws-ted-prd-edpp-airflow-cc-analytics-usbu-us-east-1/cluster_logs",
      "region": "us-east-1"
    }
  },

  "aws_attributes": {
    "zone_id": "auto",
    "availability": "SPOT_WITH_FALLBACK",
    "first_on_demand": 2,
    "instance_profile_arn": "arn:aws:iam::671616840389:role/TEC-EC2-DATABRICKS-USprd-DE-USBU-CONTRACT-CLASSIFICATION-ROLE"
  },

  "custom_tags": {
    "business-unit": "USBU",
    "environment-id": "prd",
    "app": "Contracts Analytics",
    "application-s": "2"
  },

  "init_scripts": [
    {
      "workspace": {
        "destination": "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/Cluster_Files/cdp_init_script.sh"
      }
    }
  ]
}


# Default args for the DAG
default_args = {
    'owner': dag_owner,
    'start_date': dag_creation_date,
    'email': dag_owner_email,
    'retries': no_of_retries,
    'retry_delay': timedelta(seconds=retry_delay),
    'execution_timeout': timedelta(seconds=execution_timeout),
    'end_date': end_date,
    'sla': timedelta(minutes=sla_time)  # SLA defined for the entire DAG
}

# DAG definition
dag = DAG(
    dag_id=dag_name,                            # Dynamic DAG ID which is Batch_ID
    schedule_interval=schedule,                 # Dynamic scheduling
    default_args=default_args,                  # Use default_args
    catchup=False,                              # No backfilling
    tags=[dag_tag])                            # Tag the DAG

# Retrieve the connection using Airflow's BaseHook (Connection to Databricks)
connection = BaseHook.get_connection(databricks_connection_id)

# Extract Databricks credentials from the connection object
databricks_url = connection.host            # Databricks URL.
databricks_token = connection.password      # Databricks API Token.


# Task to submit a Databricks job for batch execution
batch_start_control = DatabricksSubmitRunOperator(
    task_id='batch_start_control',
    databricks_conn_id=databricks_connection_id,
    # Cluster ID from start_cluster task
    existing_cluster_id="{{ task_instance.xcom_pull(task_ids='start_cluster') }}",
    run_name='Batch_Start_Job',
    notebook_task=batch_start_control_params,  # Parameters for the notebook task
    do_xcom_push=True,
    dag=dag
)

# Job Execution Details (this could be a dynamic placeholder to fetch job details)
task_CONTRACT_ANALYTICS_CDP_PROCESS_PDF_TO_TEXT_EXTRACTION = DatabricksSubmitRunOperator(
                            task_id = 'CONTRACT-ANALYTICS-CDP-PROCESS-PDF-TO-TEXT-EXTRACTION',                                            
                            databricks_conn_id = databricks_connection_id,
                            notebook_task = {
                                "notebook_path" : "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
                                "base_parameters" : {"job_id": "CONTRACT-ANALYTICS-CDP-PROCESS-PDF-TO-TEXT-EXTRACTION", "job_name": "CONTRACT-ANALYTICS-CDP-PROCESS-PDF-TO-TEXT-EXTRACTION", "batch_name": "contract_analytics_cdp_pdf_to_text_extraction", "user_id": "varun.potlacheruvu@takeda.com", "app_id": "contract_analytics_cdp_prod_airflow", "entity_id": "CONTRACT-ANALYTICS-CDP-PROCESS-PDF-TO-TEXT-EXTRACTION", "sns_topic_arn": SNS_TOPIC_ARN, "mwaa_region": MWAA_REGION, "mwaa_env_name": MWAA_ENV_NAME, "mwaa_iam_role": MWAA_IAM_ROLE, "custom_notebook_path": "/Workspace/Repos/CONTRACT-CLASSIFICATION-REPOSITORY/contract-classification-codebase/CC_code_CN/pdf_to_txt_CN", "custom_notebook_param": ""},
                                "source" : "WorkSpace"
                            },
                            existing_cluster_id = "{{ task_instance.xcom_pull(task_ids='start_cluster') }}", 
                            run_name = 'CONTRACT_ANALYTICS_CDP_PROCESS_PDF_TO_TEXT_EXTRACTION',
                            do_xcom_push = True,
                            dag=dag
                        )
task_CONTRACT_CDP_NOTEBOOK_DATA_SPECIFICATION_EXTRACTION = DatabricksSubmitRunOperator(
                            task_id = 'CONTRACT-CDP-NOTEBOOK-DATA-SPECIFICATION-EXTRACTION',                                            
                            databricks_conn_id = databricks_connection_id,
                            notebook_task = {
                                "notebook_path" : "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
                                "base_parameters" : {"job_id": "CONTRACT-CDP-NOTEBOOK-DATA-SPECIFICATION-EXTRACTION", "job_name": "CONTRACT-CDP-NOTEBOOK-DATA-SPECIFICATION-EXTRACTION", "batch_name": "contract_analytics_cdp_pdf_to_text_extraction", "user_id": "varun.potlacheruvu@takeda.com", "app_id": "contract_analytics_cdp_prod_airflow", "entity_id": "CONTRACT-CDP-NOTEBOOK-DATA-SPECIFICATION-EXTRACTION", "sns_topic_arn": SNS_TOPIC_ARN, "mwaa_region": MWAA_REGION, "mwaa_env_name": MWAA_ENV_NAME, "mwaa_iam_role": MWAA_IAM_ROLE, "custom_notebook_path": "/Workspace/Repos/CONTRACT-CLASSIFICATION-REPOSITORY/contract-classification-codebase/CC_code_CN/data_specification_attribute_extraction_CN", "custom_notebook_param": ""},
                                "source" : "WorkSpace"
                            },
                            existing_cluster_id = "{{ task_instance.xcom_pull(task_ids='start_cluster') }}", 
                            run_name = 'CONTRACT_CDP_NOTEBOOK_DATA_SPECIFICATION_EXTRACTION',
                            do_xcom_push = True,
                            dag=dag
                        )
task_CONTRACT_CDP_NOTEBOOK_CUSTOMER_DETAILS_EXTRACTION = DatabricksSubmitRunOperator(
                            task_id = 'CONTRACT-CDP-NOTEBOOK-CUSTOMER-DETAILS-EXTRACTION',                                            
                            databricks_conn_id = databricks_connection_id,
                            notebook_task = {
                                "notebook_path" : "/Workspace/Repos/CORP-CDP/CDP-Framework-V2/CDP_AUTOMATION/CDP_Framework/Wrapper",
                                "base_parameters" : {"job_id": "CONTRACT-CDP-NOTEBOOK-CUSTOMER-DETAILS-EXTRACTION", "job_name": "CONTRACT-CDP-NOTEBOOK-CUSTOMER-DETAILS-EXTRACTION", "batch_name": "contract_analytics_cdp_pdf_to_text_extraction", "user_id": "varun.potlacheruvu@takeda.com", "app_id": "contract_analytics_cdp_prod_airflow", "entity_id": "CONTRACT-CDP-NOTEBOOK-CUSTOMER-DETAILS-EXTRACTION", "sns_topic_arn": SNS_TOPIC_ARN, "mwaa_region": MWAA_REGION, "mwaa_env_name": MWAA_ENV_NAME, "mwaa_iam_role": MWAA_IAM_ROLE, "custom_notebook_path": "/Workspace/Repos/CONTRACT-CLASSIFICATION-REPOSITORY/contract-classification-codebase/CC_code_CN/customer_attribute_extraction_CN", "custom_notebook_param": ""},
                                "source" : "WorkSpace"
                            },
                            existing_cluster_id = "{{ task_instance.xcom_pull(task_ids='start_cluster') }}", 
                            run_name = 'CONTRACT_CDP_NOTEBOOK_CUSTOMER_DETAILS_EXTRACTION',
                            do_xcom_push = True,
                            dag=dag
                        )
start_cluster_task = PythonOperator(
                                        task_id='start_cluster',
                                        python_callable=start_cluster,
                                        op_kwargs={
                                            'databricks_instance': databricks_url,
                                            'databricks_conn_id': databricks_connection_id,
                                            'cluster_id' : cluster_id,
                                            'cluster_config' : cluster_config
                                        },
                                        do_xcom_push=True,
                                        dag=dag
                                    )
terminate_cluster_task = PythonOperator(
                                            task_id='terminate_cluster',
                                            python_callable=stop_cluster,
                                            op_kwargs={
                                                'databricks_instance': databricks_url,
                                                'databricks_conn_id': databricks_connection_id,
                                                'cluster_id' : "{{ task_instance.xcom_pull(task_ids='start_cluster') }}",
                                            },
                                            dag=dag
                                        ) 

# Task to stop the Databricks job after completion
batch_stop_control = DatabricksSubmitRunOperator(
    task_id='batch_stop_control',
    databricks_conn_id=databricks_connection_id,
    # Use the cluster started previously. 
    existing_cluster_id="{{ task_instance.xcom_pull(task_ids='start_cluster') }}",
    run_name='Batch_Stop_Job',
    notebook_task=batch_stop_control_params,  # Parameters for the stop control task.
    do_xcom_push=True,
    dag=dag
)


# Set the task dependencies
start_cluster_task >> batch_start_control >> task_CONTRACT_CDP_NOTEBOOK_DATA_SPECIFICATION_EXTRACTION >> task_CONTRACT_CDP_NOTEBOOK_CUSTOMER_DETAILS_EXTRACTION >> task_CONTRACT_ANALYTICS_CDP_PROCESS_PDF_TO_TEXT_EXTRACTION >> batch_stop_control >> terminate_cluster_task