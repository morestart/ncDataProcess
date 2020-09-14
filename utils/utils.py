import os

import numpy as np
import pandas as pd
import xarray as xr


def load_nc_data(path: str):
    """
    加载nc格式数据

    :param path: nc data path
    :return: ds : Dataset
    """

    # 后缀不是.nc格式直接释放异常
    assert path[-2:] == 'nc', '文件格式必须为.nc'
    ds = xr.open_dataset(path)
    return ds


def get_nc_attr(dataset):
    """
    获取nc数据中的属性

    :param dataset: 数据集
    :return: attr[list]
    """
    attr = list()
    for i in dataset.data_vars:
        attr.append(i)
    return attr


def get_lon_lat(dataset):
    """
    获取经纬度数据

    :param dataset: 数据集
    :return: lon[list]: 纬度, lat[list]: 经度
    """
    lon = dataset.longitude.values
    lat = dataset.latitude.values
    return lon, lat


def get_part_lat_lon(dataset, lat_min_max: tuple, lon_min_max: tuple):
    """
    截取部分地区经纬度

    :param dataset: 数据集
    :param lat_min_max: 纬度最大最小元组 eg: (34.75, 35.5)
    :param lon_min_max: 经度最大最小元组 eg: (120.5, 123.5)
    :return: lat_need[list], lon_need[list]
    """

    lon, lat = get_lon_lat(dataset)
    lat_index = np.where((lat <= lat_min_max[1]) & (lat >= lat_min_max[0]))
    lon_index = np.where((lon <= lon_min_max[1]) & (lon >= lon_min_max[0]))

    lat_need = list()
    lon_need = list()
    for lo_index, la_index in zip(lon_index, lat_index):
        lat_need.append(lat[la_index])
        lon_need.append(lon[lo_index])
    return lat_need, lon_need


def get_time(dataset):
    """
    获取时间

    :param dataset: 数据集
    :return: dataset[attr[1]]: DataArray
    """
    attr = get_nc_attr(dataset)
    assert attr.__len__() == 2, '属性超出两个'
    # t = dataset[attr[1]]['time'].values
    return dataset[attr[1]]['time'].values


def get_data(dataset, lat, lon):
    """
    获取特定经纬度下的数据信息

    :param dataset: 数据集
    :param lon: 经度
    :param lat: 纬度
    :return:
    """
    attr = get_nc_attr(dataset)
    d = dataset.sel(longitude=lon, latitude=lat, method='nearest')
    return d[attr[1]].values


def make_dataframe(data, time, csv_name, data_column_name, subdir_name):
    """
    制作DataFrame并写入csv

    :param data: 数据
    :param time: 时间数据
    :param csv_name: 文件名称
    :param data_column_name: 列名
    :param subdir_name: 子文件夹名称
    :return:
    """
    processed_path = os.path.join('processed_data', subdir_name)
    try:
        df = pd.read_csv(os.path.join(processed_path, csv_name), index_col=0)
        df[data_column_name] = data

        df.to_csv(os.path.join(processed_path, csv_name))
    except FileNotFoundError:
        data_dict = {'time': time, data_column_name: data}
        df = pd.DataFrame(data_dict)
        df.to_csv(os.path.join(processed_path, csv_name))


def make_csv_file(dataset, lat, lon, subdir_name):
    """
    生成CSV文件

    :param dataset: 数据集
    :param lat: 纬度
    :param lon: 经度
    :param subdir_name: 子文件夹名称
    :return:
    """
    lat = lat[0].tolist()
    lon = lon[0].tolist()

    t = get_time(dataset)
    attr = get_nc_attr(dataset)

    for i in lat:
        for j in lon:
            data = get_data(dataset, i, j)
            make_dataframe(data, t, f'lat-{i}lon-{j}.csv', attr[1], subdir_name)


# if __name__ == '__main__':
#     d = load_nc_data(r'G:\nc\04\ww3.201004_dp.nc')
#     _lat, _lon = get_part_lat_lon(d, (34.75, 35.5), (120.5, 123.5))
#     make_csv_file(d, _lat, _lon)
