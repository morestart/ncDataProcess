import os
import shutil
from utils.utils import OperateFiles

if __name__ == '__main__':

    data_root_path = 'raw_data'

    try:
        os.mkdir('processed')
    except FileExistsError:
        shutil.rmtree('processed')
        os.mkdir('processed')

    o = OperateFiles((120.5, 121), (35.0, 35.5))
    o.read_data('raw_data')
