'''
=================================================
Milestone 3

Nama  : Irfan Risqy
Batch : FTDS-001-BSD

Program ini dibuat untuk melakukan loading data dari PostgreSQL, cleaning data dan transfer data ke Elasticsearch. Dataset yang digunakan adalah tentang 
kepuasan passanger dari suatu maskapai yang didapatkan dari kaggle.com
=================================================
'''


# Import Libraries
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch


# Akses PostgreSQL
db_user = "postgres"
db_password = "12345" 
db_host = "localhost" 
db_port = "5432" 

connection = db.connect(
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port,
    database="db_phase2"
)


# Import data dari PostgreSQL (menghilangkan duplicate dengan DISTINCT)
select_query = '''
                SELECT DISTINCT *
                FROM table_M3;'''
data_raw = pd.read_sql_query(select_query, connection)


# Mengubah tipe data kolom Departure & Arrival Delay in Minutes dari float menjadi int
data_raw['Departure Delay in Minutes'] = data_raw['Departure Delay in Minutes'].astype('Int64')
data_raw['Arrival Delay in Minutes'] = data_raw['Arrival Delay in Minutes'].astype('Int64')


# Normalisasi nama kolom menjadi lowercase
data_raw.columns = data_raw.columns.str.lower()


# Mengganti spasi dengan underscore pada nama kolom
data_raw.columns = data_raw.columns.str.replace(' ', '_')


# Menghapus simbol '-' pada kolom 'on-board'
data_raw.columns = data_raw.columns.str.replace('-', '')


'''
Untuk data ini tidak terdapat missing values oleh karena itu proses cleaning hanya meliputi perubahan dtype, 
merubah nama kolom menjadi lower case, megubah spasi menjadi underscore dan menghilangkan simbol yang tidak penting.
'''


# Save data setelah cleaning
data_raw.to_csv('P2M3_irfan_risqy_data_clean.csv', index=False)
print("-------Data Saved------")


# Upload data ke Elasticseach
es = Elasticsearch()
df = pd.read_csv('P2M3_irfan_risqy_data_clean.csv')

for i,r in df.iterrows():
     doc=r.to_json()
     res=es.index(index="p2m3", doc_type="doc", body=doc)
     print(res)


'''
Data yang sudah clean setelah itu ditransfer ke Elasticsearch dengan nama 'p2m3', 
untuk selanjutnya dilakukan visualisasi menggunakan Kibana.
'''