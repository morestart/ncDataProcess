import os

import pandas as pd

file_list = ['lat-34.75lon-120.5.csv', 'lat-34.75lon-120.75.csv', 'lat-34.75lon-121.0.csv', 'lat-34.75lon-121.25.csv',
             'lat-34.75lon-121.5.csv', 'lat-34.75lon-121.75.csv', 'lat-34.75lon-122.0.csv', 'lat-34.75lon-122.25.csv',
             'lat-34.75lon-122.5.csv', 'lat-34.75lon-122.75.csv', 'lat-34.75lon-123.0.csv', 'lat-34.75lon-123.25.csv',
             'lat-34.75lon-123.5.csv', 'lat-35.0lon-120.5.csv', 'lat-35.0lon-120.75.csv', 'lat-35.0lon-121.0.csv',
             'lat-35.0lon-121.25.csv', 'lat-35.0lon-121.5.csv', 'lat-35.0lon-121.75.csv', 'lat-35.0lon-122.0.csv',
             'lat-35.0lon-122.25.csv', 'lat-35.0lon-122.5.csv', 'lat-35.0lon-122.75.csv', 'lat-35.0lon-123.0.csv',
             'lat-35.0lon-123.25.csv', 'lat-35.0lon-123.5.csv', 'lat-35.25lon-120.5.csv', 'lat-35.25lon-120.75.csv',
             'lat-35.25lon-121.0.csv', 'lat-35.25lon-121.25.csv', 'lat-35.25lon-121.5.csv', 'lat-35.25lon-121.75.csv',
             'lat-35.25lon-122.0.csv', 'lat-35.25lon-122.25.csv', 'lat-35.25lon-122.5.csv', 'lat-35.25lon-122.75.csv',
             'lat-35.25lon-123.0.csv', 'lat-35.25lon-123.25.csv', 'lat-35.25lon-123.5.csv', 'lat-35.5lon-120.5.csv',
             'lat-35.5lon-120.75.csv', 'lat-35.5lon-121.0.csv', 'lat-35.5lon-121.25.csv', 'lat-35.5lon-121.5.csv',
             'lat-35.5lon-121.75.csv', 'lat-35.5lon-122.0.csv', 'lat-35.5lon-122.25.csv', 'lat-35.5lon-122.5.csv',
             'lat-35.5lon-122.75.csv', 'lat-35.5lon-123.0.csv', 'lat-35.5lon-123.25.csv', 'lat-35.5lon-123.5.csv']

subdir = os.listdir('processed_data')
df = pd.DataFrame()
for j in file_list:
    for i in subdir:
        if i != 'merge_file':
            d = pd.read_csv(f'processed_data/{i}/{j}', index_col=0)
            df = pd.concat([df, d])

    df.to_csv(os.path.join(r'E:\PycharmProjects\ncDataProcess\processed_data\merge_file', j), index=False)
    df = pd.DataFrame()
