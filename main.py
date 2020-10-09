import os
import shutil
from utils.utils import OperateFiles
import yaml

if __name__ == '__main__':

    with open('config.yml', 'r') as f:
        data = f.read()

    content = yaml.load(data, Loader=yaml.FullLoader)

    data_root_path = content['root_path']

    try:
        os.mkdir('processed')
    except FileExistsError:
        shutil.rmtree('processed')
        os.mkdir('processed')

    o = OperateFiles(content['lon'], content['lat'])
    o.read_data('raw_data')
