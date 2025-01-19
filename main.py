import sqlalchemy
from sqlalchemy import Table, Column, Integer, Float, String, Date, MetaData
from sqlalchemy import create_engine
from datetime import date

engine = create_engine('sqlite:///database.db')
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


#Ręczne polecenie INSERT - testowanie poprawności utworzonej bazy danych
###################################
#tworzenie polecenia SQL
ins01_stations = stations.insert().values(station="USC00534397", latitude = 21.2716, longitude = -157.8168, elevation = 3.0, name = "WAIKIKI 717.2", country = "US", state = "HI")
#print(ins01_stations) # wydruk polecenia w SQL
#print(ins01_stations.compile().params)  # wydruk parametrów polecenia SQL 'ins'
ins01_measure = measure.insert().values(station = "USC00534397", date = "2010-01-01", precip = 0.08, tobs = 65)
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
for r in result:
   print(r)

s = stations.select().where(stations.c.id < 4)
result = conn.execute(s)
print("Station, ID<2: ")
for row in result:
   print(row)   


