# import yang digunakan
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch

# function untuk mengkoneksikan pada database postgres
def get_data_from_postgresql(host, database, table, username, password, port=5432):
   
   conn_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
   conn = create_engine(conn_string)
   df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
   return df

# penggunaan fungsi
host = 'localhost'
database = 'raw_data'
table = 'table_gc7'
username = 'postgres'
password = 'postgres'

df = get_data_from_postgresql(host, database, table, username, password)
data = df.copy()
print(data.head())

# function untuk cleaning data
def preprocess_car_data(cleaning):
    # menghapus value NaN
    cleaning.dropna(inplace=True)

    # ubah tipe data kolom yang diinginkan
    # cleaning['mileage (km)'] = cleaning['mileage (km)'].astype(int)
    # cleaning['instalment (Rp|Monthly)'] = cleaning['instalment (Rp|Monthly)'].str.replace('Rp', '').str.replace(',', '').astype(int)

    # ubah nama kolom
    cleaning = cleaning.rename(columns={
        'car name': 'car_name',
        'mileage (km)': 'mileage',
        'plate type': 'plate_type',
        'rear camera': 'rear_camera',
        'sun roof': 'sun_roof',
        'auto retract mirror': 'auto_retract_mirror',
        'electric parking brake': 'electric_parking_brake',
        'map navigator': 'map_navigator',
        'vehicle stability control': 'vehicle_stability_control',
        'keyless push start': 'keyless_push_start',
        'sports mode': 'sports_mode',
        '360 camera view': 'camera_view_360',
        'power sliding door': 'power_sliding_door',
        'auto cruise control': 'auto_cruise_control',
        'price (Rp)': 'price',
        'instalment (Rp|Monthly)': 'instalment'
    })

    return cleaning

processed_data = preprocess_car_data(data)
# menampilkan 5 data yang sudah clean
print(processed_data.head())
# menyimpan data yang sudah clean menjadi file csv
processed_data.to_csv("P2G7_zaky_ramdhani_data_clean.csv") # data yang sudah clean kembali disimpan
print('\n')
print("Data Tersimpan")

# function untuk migrating file csv yang sudah clean pada elastic
def migrating_to_elasticsearch(dataframe, index_name, es_url='http://localhost:9200'):
    es = Elasticsearch(es_url)

    for i, row in dataframe.iterrows():
        doc = row.to_json()
        es.index(index=index_name, id=i + 1, body=doc)

    print('\n')
    print('-'*10,'Finished Migrating Data','-'*10)

# meload data menjadi dataframe lalu migrating pada elastic
new_data = pd.read_csv("P2G7_zaky_ramdhani_data_clean.csv")
migrating_to_elasticsearch(new_data, index_name='carslist')