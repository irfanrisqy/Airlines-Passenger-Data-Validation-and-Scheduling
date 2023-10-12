'''
=================================================
Milestone 3

Nama  : Irfan Risqy
Batch : FTDS-001-BSD

Program ini dibuat untuk membuat program automasi connect data dengan PostgreSQL, cleaning data dan transfer data ke Elasticsearch dengan menggunakan Airflow.
=================================================
'''

# Import Libraries
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2 
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Akses PostgreSQL
def FetchData():
    # Access PostgreSQL
    db_user = "airflow"
    db_password = "airflow" 
    db_host = "postgres" 
    db_port = "5432" 
    db = "airflow"

    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db
    )
 
    # Query SQL untuk mengambil data dari tabel
    raw = "SELECT * FROM table_m3;"

    # Menjadikan hasil query menjadi dataframe
    df = pd.read_sql(raw, connection)

    # Save hasil query ke csv
    df.to_csv('/opt/airflow/dags/datam3.csv', index=False)

'''
Setelah data telah didapatkan dari PostgreSQL, langkah selanjutnya adalah cleaning data yang telah didapat tadi.
'''

    
# Clean Data
def CleanData():
    # Load csv
    data_raw = pd.read_csv('/opt/airflow/dags/datam3.csv')

   # Mengubah tipe data kolom Departure & Arrival Delay in Minutes dari float menjadi int
    data_raw['Departure Delay in Minutes'] = data_raw['Departure Delay in Minutes'].astype('Int64')
    data_raw['Arrival Delay in Minutes'] = data_raw['Arrival Delay in Minutes'].astype('Int64')

    # Normalisasi nama kolom menjadi lowercase
    data_raw.columns = data_raw.columns.str.lower()

    # Mengganti spasi dengan underscore pada nama kolom
    data_raw.columns = data_raw.columns.str.replace(' ', '_')

    # Menghapus simbol '-' pada kolom 'on-board'
    data_raw.columns = data_raw.columns.str.replace('-', '')

    # Save data setelah cleaning
    data_raw.to_csv('/opt/airflow/dags/P2M3_irfan_risqy_data_clean.csv', index=False)

'''
Untuk data ini tidak terdapat missing values oleh karena itu proses cleaning hanya meliputi perubahan dtype, 
merubah nama kolom menjadi lower case, megubah spasi menjadi underscore dan menghilangkan simbol yang tidak penting. Setelah data sudah bersih,
setelah itu akan dilakukan transfer data ke Elasticsearch.
'''

# Post Kibana
def PostKibana():
        # Konfigurasi koneksi ke Kibana/Elasticsearch
        es = Elasticsearch("http://elasticsearch:9200") 

        # Menyimpang hasil data cleaning
        df= pd.read_csv('/opt/airflow/dags/P2M3_irfan_risqy_data_clean.csv')

        # Documents dan actions
        documents = df.to_dict(orient='records')
        actions = [
            {
                "_op_type": "index",  # Jenis operasi (indeks)
                "_index": "p2m3",  # Nama indeks
                "_source": doc  # Isi dokumen (JSON)
            }
            for doc in documents
        ]

        # Menggunakan metode bulk untuk mengirimkan indeks ke Elasticsearch
        response = helpers.bulk(es, actions)
        print(response)

# Argument dan Scheduling Untuk Airflow
default_args = {
    'owner': 'irfan',
    'start_date': dt.datetime(2023, 10, 4, 16, 20, 0)-dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=2),
}

'''
Setelah sudah melakukan proses connect dengan PostgreSQL, proses Cleaning Data dan proses transfer data ke Elasticsearch,
langkah selanjutnya adalah setting tahapan yang tadi sudah dibuat kedalam Airflow dan melakukan scheduling.
'''

# Tahapan Airflow
with DAG('Milestone3',
         default_args=default_args,
         schedule_interval=timedelta(minutes=2),    
         ) as dag:

    Fetch = PythonOperator(task_id='fetch',
                                 python_callable=FetchData)
    
    Clean = PythonOperator(task_id='clean',
                                 python_callable=CleanData)

    Post = PythonOperator(task_id='post',
                                 python_callable=PostKibana)
    

Fetch >> Clean >> Post
 
'''
Bisa dilihat untuk airflow kali ini tahapannya adalah Fetch yaitu fetch dengan Postgre, setelah itu Clean yang berarti cleaning data dan Post
yang berarti transfer data ke Kibana. Untuk schedulingnya pada program kali ini akan dilakukan proses setiap 2 menit.
'''