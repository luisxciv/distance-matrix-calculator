import pandas as pd
import numpy as np
import pymysql.cursors

class Colour:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

def haversine(lon1, lat1, lon2, lat2):
    #  conver degrees to radians
    lon1 = np.deg2rad(lon1)
    lat1 = np.deg2rad(lat1)
    lon2 = np.deg2rad(lon2)
    lat2 = np.deg2rad(lat2)

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r_e = 6371
    return c * r_e

rdsConn = pymysql.connect(host = '127.0.0.1',
                          db = 'napify',
                          user = 'root',
                          password = '',
                          charset = 'utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

cursor1 = rdsConn.cursor()
cursor2 = rdsConn.cursor()
cursor3 = rdsConn.cursor()

sql = "select distinct mobile_user_id from score where speed_range_id > 1"
speed_query = "select speed_range_id, count(speed_range_id) from score where mobile_user_id = %(mobile_user_id)s and speed_range_id in (2, 9, 10, 11, 12 ,13) group by speed_range_id"
distance_query = """SELECT created_date, latitude, longitude FROM score s where s.mobile_user_id = %(mobile_user_id)s and speed_range_id > 1 group by latitude, longitude order by id asc"""
insert_query = "Update mobile_user set average_speed, average_distance, average_time "
#col_names = ['created_date', 'latitude', 'longitude']

cursor1.execute(sql)
result = cursor1.fetchall()

for row1 in result:
    cursor2.execute(speed_query, row1)
    distance = cursor3.execute(distance_query, row1)
    speed_result = cursor2.fetchall()
    distance_result = cursor3.fetchall()
    for row2 in speed_result:
        id_vals = {2: 25, 9: 50, 10: 75, 11: 100, 12: 150, 13: 15}
        result_sum = sum(id_vals[row['speed_range_id']] * row['count(speed_range_id)'] for row in speed_result)
        count_sum = sum(row['count(speed_range_id)'] for row in speed_result)
        avg_speed = result_sum / count_sum
    #if you want to read th
    #df = pd.read_csv('file.csv', names=col_names, sep=',', skiprows=1)

    df = pd.DataFrame(distance_result)
    #merge our data
    m = df.reset_index().merge(df.reset_index(), on='created_date')
    m = m[m.index_x != m.index_y].drop(columns=['index_x', 'index_y'])

    #change our dataset from object type to float
    m["longitude_x"] = m.longitude_x.astype(float)
    m["longitude_y"] = m.longitude_y.astype(float)
    m["latitude_x"] = m.latitude_x.astype(float)
    m["latitude_y"] = m.latitude_y.astype(float)

    #Calculate Distance
    m['Distance'] = haversine(m.longitude_x, m.latitude_x, m.longitude_y, m.latitude_y)

    #Count the number if ID's
    count = df.created_date.count()
    grouped = m.groupby('created_date').Distance.mean()
    days = pd.to_datetime(m.iloc[:, 0]).dt.normalize().nunique()
    total = sum(grouped)
    try:
        average_daily = total / days
    except ZeroDivisionError:
        average_daily = 0
    days2 = pd.to_datetime(df.iloc[:, 0]).dt.normalize().nunique()
    try:
        average_time = count * 3 / days2
    except ZeroDivisionError:
        average_time = 0

    print("ID de usuario" + str(row1))
    print("Distancia recorrida promedio de " + Colour.RED + str(round(average_daily, 1)) + Colour.END + " km al dia")
    print("Tiempo manejado promedio de " + Colour.RED + str(round(average_time, 1)) + Colour.END + " minutos al dia")
    print("Velocidad promedio de " + Colour.RED + str(round(avg_speed, 1)) + Colour.END + " km por hora")
    print(Colour.RED + "=========================================" + Colour.END)








