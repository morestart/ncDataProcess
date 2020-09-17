from utils.utils import *
from tqdm import tqdm


if __name__ == '__main__':

    start_lon = (120.5, 124.5)
    start_lat = (34.75, 36.5)
    root_path = 'raw_data'
    process_path = 'processed_data'
    all_files_path_list, subdir_list = get_all_file(root_path)

    try:
        os.mkdir(process_path)
    except FileExistsError:
        os.remove(process_path)
    finally:
        for file, sub in tqdm(zip(all_files_path_list, subdir_list), total=len(subdir_list)):
            # 获取全球数据集
            dataset = load_nc_data(file)
            # 获取区间内部分经纬度
            _lat, _lon = get_part_lat_lon(dataset, start_lat, start_lon)
            # 制作csv文件
            make_csv_file(dataset, _lat, _lon, sub)
        # 按月合并csv文件
        merge_csv_file(process_path)

