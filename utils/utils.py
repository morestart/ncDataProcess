import os
import pandas as pd
import xarray as xr
import numpy as np
from tqdm import tqdm
import sys


class NC:

    @staticmethod
    def load_nc_data(path: str):
        """
        加载nc数据
        :param path: file path
        :return: ds: dataset
        """
        assert path[-2:] == 'nc', 'file format must be .nc'
        ds = xr.open_dataset(path)
        return ds

    @staticmethod
    def get_nc_attributes(dataset):
        """
        获取数据集中的属性
        :param dataset: 数据集
        :return:
        """
        attr = list()
        for i in dataset.data_vars:
            attr.append(i)
        return attr

    @staticmethod
    def get_earth_all_lon_lat(dataset):
        """
        获取全球的经纬度数据
        :param dataset: 数据集
        :return: lon, lat: 经度, 纬度
        """
        lon = dataset.longitude.values
        lat = dataset.latitude.values
        return lon, lat

    def get_part_lon_lat(self, dataset, lon_min_max: tuple, lat_min_max: tuple):
        """
        截取部分地区经纬度
        :param dataset: 数据集
        :param lon_min_max: 经度最大最小元组 eg: (120.5, 123.5)
        :param lat_min_max: 纬度最大最小元组 eg: (34.75, 35.5)
        :return: lat_need[list], lon_need[list]
        """
        lon, lat = self.get_earth_all_lon_lat(dataset)
        lon_index = np.where((lon <= lon_min_max[1]) & (lon >= lon_min_max[0]))
        lat_index = np.where((lat <= lat_min_max[1]) & (lat >= lat_min_max[0]))

        lat_need = list()
        lon_need = list()
        for lo_index, la_index in zip(lon_index, lat_index):
            lat_need.append(lat[la_index])
            lon_need.append(lon[lo_index])

        return lon_need, lat_need

    @staticmethod
    def get_time(dataset):
        """
        获取时间信息
        :return:
        """
        return dataset.time.values

    def get_part_data(self, dataset, lon, lat):
        """
        获取部分经纬度下的数据
        :param dataset: 数据集
        :param lon: 单点经度
        :param lat: 单点纬度
        :return:
        """
        # 获取一个文件内所有的变量 (有些文件可能会有多个 比如风 u\v)
        attr = self.get_nc_attributes(dataset)
        # 移除MAPSTA
        attr.remove('MAPSTA')
        # 获取固定经纬度下的数据
        d = dataset.sel(longitude=lon, latitude=lat, method='nearest')
        # 因为可能有很多属性,所以用字典保存,key是属性,value是值
        need_data = dict()
        for item in attr:
            need_data[item] = d[item].values

        return need_data


class OperateFiles:

    def __init__(self, part_lon, part_lat):
        self._nc = NC()
        self._part_lon = part_lon
        self._part_lat = part_lat

    @staticmethod
    def get_all_file(root_path):
        """
        获得根目录下所有文件
        :param root_path: 根目录
        :return: all_files_path_list[list]
        """
        # 三个返回值
        # folder: 全部的文件路径
        # subs: 子文件夹列表
        # files: 最深目录下的文件
        all_files_path_list = list()
        sub_dir_list = list()
        flag = True
        for folder, subdir, files in os.walk(root_path):
            if not flag:
                for item in files:
                    full_file_path = os.path.join(folder, item)
                    all_files_path_list.append(full_file_path)
            flag = False
            for name in subdir:
                sub_dir_list.append(os.path.join(folder, name))

        return all_files_path_list, sub_dir_list

    def get_nc_file(self, root_path):
        """
        获取按月份分类的nc数据
        :param root_path:
        :return:
        """
        all_files, subdir = self.get_all_file(root_path)
        d = dict()
        for item in subdir:
            try:
                if sys.platform != 'win32':
                    os.mkdir(f'processed/{item.split("/")[1]}')
                else:
                    os.mkdir(os.path.join('processed', item.split("\\")[1]))
            except FileExistsError:
                pass
            finally:
                subdir_files = os.listdir(item)
            cache = list()
            for file in subdir_files:
                cache.append(os.path.join(item, file))

            if sys.platform != 'win32':
                d[item.split("/")[1]] = cache
            else:
                d[item.split("\\")[1]] = cache
        return d

    def read_data(self, root_path):
        all_nc_file = self.get_nc_file(root_path)

        # 遍历文件夹
        for i in tqdm(range(1, len(all_nc_file) + 1)):
            # 遍历文件夹内所有文件
            for file in all_nc_file[f'{i}']:
                # 读取nc数据
                dataset = self._nc.load_nc_data(file)
                # 获取一个区域内的所有可选经纬度
                lon, lat = self._nc.get_part_lon_lat(dataset, self._part_lon, self._part_lat)
                _time = self._nc.get_time(dataset)
                # 循环读取经纬度,获取所有经纬度格点 并读取该格点的数据
                lon = lon[0]
                lat = lat[0]
                for single_lon in lon:
                    for single_lat in lat:
                        # 获取数据 data是一个字典 字典内可能有多个元素
                        data = self._nc.get_part_data(dataset, single_lon, single_lat)
                        # 获取字典的键
                        attributes = data.keys()

                        for k in attributes:
                            csv_name = f"{single_lon}-{single_lat}.csv"
                            self.append2csv(_time, data[k], k, i, csv_name)

    @staticmethod
    def append2csv(data_time, data, column_name, subdir, csv_name):
        """
        追加数据到csv文件
        :param data_time: 时间列
        :param data: 数据列
        :param column_name: 列名
        :param subdir: 保存的子文件夹名
        :param csv_name: csv文件名
        :return:
        """
        processed_path = 'processed'
        csv_saved_path = os.path.join(os.path.join(processed_path, str(subdir)), csv_name)

        try:
            df = pd.read_csv(csv_saved_path, index_col=0)
            if column_name not in df.columns:
                df[column_name] = data
                df.to_csv(csv_saved_path)
        except FileNotFoundError:

            data_dict = {'time': data_time, column_name: data}
            df = pd.DataFrame(data_dict)
            df.to_csv(csv_saved_path)
