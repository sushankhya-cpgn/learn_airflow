from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import zipfile
from minio import Minio
from airflow.operators.email import EmailOperator



def process_task(**context):

    # Get Filename from Webhook trigger
    filename = context['dag_run'].conf.get('file_name') 
    print(f"File name received from {filename}")

    #Minio initialization

    client = Minio(
        "minio:9000",
        access_key = "minio",
        secret_key = "minio123",
        secure = False
    )

    bucket_name = "mybucket"
    print(os.path.join('/usr/local/airflow/input', os.path.basename(filename)))
    local_file_path = os.path.join('/usr/local/airflow/input',os.path.basename(filename))

    # Download to airflow

    try:
        client.fget_object(bucket_name, filename, local_file_path)
        print("File Downloaded Successfully")

    except Exception as e:
        print(f"Error downloading the file, Error: {e}")
        raise

    context['ti'].xcom_push(key='file_path',value = local_file_path)
    return local_file_path




def compress_file(**context):

    file_path = context['ti'].xcom_pull(task_ids= 'process_task',key='file_path')
    filename = context['dag_run'].conf.get('file_name') 
    zip_file_name = os.path.join(os.path.dirname(file_path), os.path.splitext(filename)[0] + ".zip")
    print(zip_file_name)
    # new_file = file_name.join
    # zip_path = os.path.join(COMPRESS_DIR, file_name + '.zip')
    with zipfile.ZipFile(zip_file_name, 'w') as zipf:
        zipf.write(file_path, arcname=file_path)
    original_size = os.path.getsize(file_path)
    compressed_size = os.path.getsize(zip_file_name)
    context['ti'].xcom_push(key='zip_path', value=zip_file_name)
    context['ti'].xcom_push(key='original_size', value=original_size)
    context['ti'].xcom_push(key='compressed_size', value=compressed_size)

def cleanupfn(**context):
    file_path = context['ti'].xcom_pull(task_ids= 'process_task',key='file_path')
    zip_path = context['ti'].xcom_pull(task_ids = 'compress_file',key='zip_path')

    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f'Deleted File {file_path}')
        if os.path.exists(zip_path):
            os.remove(zip_path)
            print(f'Deleted Zip File {zip_path}')
    except Exception as e:
        print(f"Error during cleanup{e}")
        raise
        
    



default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}


with DAG(
    'event_driven_compress',
    default_args=default_args,
    description='Event Driven DAG that compresses uploaded file and sends email',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id = 'process_task',
        python_callable = process_task,
        dag = dag
    )

    compress_task = PythonOperator(
        task_id='compress_file',
        python_callable=compress_file,
        op_kwargs={'file_name': 'sample.txt'}
    )

    send_email = EmailOperator(
    task_id='send_email_task',
    to='sushankhya41@gmail.com',
    subject='Hello from Airflow!',
    html_content="""
    <h2>📁 New Data Uploaded</h2>
    <p>The file has been successfully processed and compressed. Please find the attachments below.</p>

    <h3>📝 File Specifications:</h3>
    <ul>
        <li><strong>Original File Size:</strong> {{ ti.xcom_pull(task_ids='compress_file', key='original_size') }} bytes</li>
        <li><strong>Compressed File Size:</strong> {{ ti.xcom_pull(task_ids='compress_file', key='compressed_size') }} bytes</li>
    </ul>

<p>Plase look into it </p>

    """,
    files=[
        "{{ ti.xcom_pull(task_ids='process_task', key='file_path') }}",
        "{{ ti.xcom_pull(task_ids='compress_file', key='zip_path') }}"
    ]
)

    send_email.template_fields = ('files','html_content')

    cleanup_files = PythonOperator(
        task_id = 'clean_files',
        python_callable = cleanupfn,
    )


    t1 >> compress_task >> send_email >> cleanup_files