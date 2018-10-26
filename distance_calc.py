import pandas as pd
import numpy as np
import paramiko
import pymysql.cursors
from sshtunnel import SSHTunnelForwarder
from paramiko import SSHClient
from os.path import expanduser

class Colour:
   YELLOW = '\033[93m'
   RED = '\033[91m'


def haversine(lon1, lat1, lon2, lat2):
    #  convert degrees to radians
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

pkeyfilepath = ''
home = expanduser('~')
mypkey = paramiko.RSAKey.from_private_key_file(home + pkeyfilepath)

##SSHTUNNEL

with SSHTunnelForwarder(
        ('', 22),
    ssh_username='',
    ssh_pkey=mypkey,
    remote_bind_address=('', 3306)) as tunnel:
    rdsConn = pymysql.connect(host='127.0.0.1',
                              db='',
                              user='',
                              password='',
                              port=tunnel.local_bind_port,
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


#Initialize cursors

    cursor1 = rdsConn.cursor()
    cursor3 = rdsConn.cursor()
    cursor4 = rdsConn.cursor()

    sql = """select distinct mobile_user_id from score 
            where speed_range_id in (2, 9, 10, 11, 12, 13) and mobile_user_id > 516 
     order by mobile_user_id asc"""
    distance_query = """SELECT created_date, latitude, longitude FROM score s where s.mobile_user_id = %(mobile_user_id)s and speed_range_id > 1 group by latitude, longitude order by id asc"""
    insert_query = "Update mobile_user set avg_speed=%s, avg_distance=%s, avg_time=%s where mobile_user_id = %(mobile_user_id)s "
    #col_names = ['created_date', 'latitude', 'longitude']

    cursor1.execute(sql)
    result = cursor1.fetchall()

    for row1 in result:
        distance = cursor3.execute(distance_query, row1)
        distance_result = cursor3.fetchall()

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

        id = int(row1["mobile_user_id"])
        print("ID de usuario: " + Colour.RED + str(int(row1["mobile_user_id"])) + Colour.END)
        print("Recorrido un total de distancia de" + Colour.RED + str(round(total, 1)) + Colour.END + " km")

        print(Colour.RED + "=================================================" + Colour.END)
        rdsConn.commit()
    rdsConn.close()

