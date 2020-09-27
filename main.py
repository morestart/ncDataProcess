import os
import shutil
from utils.utils import OperateFiles

if __name__ == '__main__':
    try:
        os.mkdir('processed')
    except FileExistsError:
        shutil.rmtree('processed')

    o = OperateFiles((120.5, 121.5), (34.75, 35.0))
    o.read_data('raw_data')
