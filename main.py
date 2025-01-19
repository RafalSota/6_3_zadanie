#import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Date, MetaData
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('sqlite:///database.db', echo=False)
meta = MetaData()

stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String)
)

measure = Table(
   'measure', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', Float),
   Column('tobs', Float)
)

meta.create_all(engine)
print(engine.table_names())
conn = engine.connect()

df = pd.read_csv('clean_stations.csv')
print("HEAD pliku clean_stations.csv: ")
print(df.head())
for index, row in df.iterrows():
   insert_data_csv_stations = stations.insert().values(station = row['station'], latitude = row['latitude'], longitude = row['longitude'], elevation = row['elevation'], name = row['name'], country = row['country'], state = row['state'])
   print(f"Importuję pozycję nr: {index}")
   conn.execute(insert_data_csv_stations)

df = pd.read_csv('clean_measure.csv')
print("HEAD pliku clean_measure.csv: ")
print(df.head())
for index, row in df.iterrows():
   insert_data_csv_measure = measure.insert().values(station = row['station'], date = row['station'], precip = row['precip'], tobs = row['tobs'])
   print(f"Importuję pozycję nr: {index}")
   conn.execute(insert_data_csv_measure)
conn.close()

#Ręczne polecenie INSERT - testowanie poprawności utworzonej bazy danych
##################################
#tworzenie polecenia SQL
ins01_stations = stations.insert().values(station="_manual_USC00534397", latitude = 21.2716, longitude = -157.8168, elevation = 3.0, name = "WAIKIKI 717.2", country = "US", state = "HI")
#print(ins01_stations) # wydruk polecenia w SQL
#print(ins01_stations.compile().params)  # wydruk parametrów polecenia SQL 'ins'
ins01_measure = measure.insert().values(station = "manual_USC00534397", date = "2010-01-01", precip = 0.08, tobs = 65)
conn = engine.connect()
#result = conn.execute(ins01_stations)
conn.execute(ins01_stations)
conn.execute(ins01_measure)

results = engine.execute("SELECT * FROM stations")
print("Stations: ")
for r in results:
   print(r)

s = measure.select()
result = conn.execute(s)
print("Measure: ")
count = 0
for r in result:        
   print(r)
   count += 1
   if count == 20:   # ograniczenie ilości wyświetleń
      break

s = stations.select().where(stations.c.id < 4)
result = conn.execute(s)
print("Station, ID<4: ")
for row in result:
   print(row)   

s = measure.select().where(measure.c.id < 50)
result = conn.execute(s)
print("Odczyty, ID<50: ")
for row in result:
   print(row)   

print("First 5 Stations: ")
rows = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
print(rows)
