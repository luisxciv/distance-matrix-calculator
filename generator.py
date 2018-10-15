import pandas as pd
import numpy as np

col_names = ['created_date', 'latitude', 'longitude']
df = pd.read_csv('marco.csv', names=col_names, sep=',', skiprows=1)



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
total = pd.to_datetime(m.iloc[:, 0]).dt.normalize().nunique()
print(total)
print(grouped)

