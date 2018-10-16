import pandas as pd
import numpy as np
import mysql.connector
import csv

with open('users.csv', newline='') as userscsv:
    for i in userscsv:
        mydb = mysql.connector.connect(
          host="localhost",
          user="root",
          passwd="",
          database="napify"
        )
        mycursor = mydb.cursor()
        query = ("SELECT name FROM napify.mobile_user where id = 1")
        mycursor.execute(query)
        myresult = mycursor.fetchall()
        print(myresult)


col_names = ['created_date', 'latitude', 'longitude']
df = pd.read_csv('pollo.csv', names=col_names, sep=',', skiprows=1)
count = df.created_date.count()

def haversine(lon1, lat1, lon2, lat2):
    # convert degrees to radians
    lon1 = np.deg2rad(lon1)
    lat1 = np.deg2rad(lat1)
    lon2 = np.deg2rad(lon2)
    lat2 = np.deg2rad(lat2)

    # formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r_e = 6371
    return c * r_e

# merge
m = df.reset_index().merge(df.reset_index(), on='created_date')

# remove comparisons of the same event
m = m[m.index_x != m.index_y].drop(columns = ['index_x', 'index_y'])

# Calculate Distance
m['Distance'] = haversine(m.longitude_x, m.latitude_x, m.longitude_y, m.latitude_y)


grouped = m.groupby('created_date').Distance.mean()
days = pd.to_datetime(m.iloc[:, 0]).dt.normalize().nunique()
total = sum(grouped)
average_daily = total/days
days2 = pd.to_datetime(df.iloc[:, 0]).dt.normalize().nunique()
average_time = count * 3/days2

print("Distancia recorrida promedio de " + str(round(average_daily, 1)) + "km al dia")
print("Tiempo manejado promedio de " + str(round(average_time, 1)) + " minutos al dia")





