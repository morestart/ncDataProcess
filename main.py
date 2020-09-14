from utils.utils import *
from tqdm import tqdm


def get_dir_list(root_dir_path):
    """
    获取文件列表
    :param root_dir_path: 根目录
    :return: list
    """
    processed_path = 'processed_data'
    for sub_dir in os.listdir(root_dir_path):
        try:
            os.mkdir(os.path.join(processed_path, sub_dir))
        except FileExistsError:
            pass
    return os.listdir(root_dir_path)


if __name__ == '__main__':

    start_lon = (120.5, 123.5)
    start_lat = (34.75, 35.5)

    root_path = 'raw_data'
    dir_list = get_dir_list(root_path)
    print('处理进度:\n')
    for subdir in tqdm(dir_list):
        file_list = os.listdir(os.path.join(root_path, subdir))
        for file in file_list:
            data = load_nc_data(os.path.join(os.path.join(root_path, subdir), file))
            _lat, _lon = get_part_lat_lon(data, start_lat, start_lon)
            make_csv_file(data, _lat, _lon, subdir)
