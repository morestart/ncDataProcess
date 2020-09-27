import os
import pandas as pd
from tqdm import tqdm


def merge_csv_file(root_path):
    # content = pd.read_csv(file_path, index_col=0)
    # df = pd.concat([df, content])
    # df.to_csv(new_file_path)
    df = pd.DataFrame()
    subdirs = os.listdir(root_path)
    for subdir in tqdm(subdirs):
        if subdir != 'merge_files':
            files = os.listdir(os.path.join(root_path, subdir))
            for file in files:
                file_path = os.path.join(os.path.join(root_path, subdir), file)
                content = pd.read_csv(file_path, index_col=0)
                df = pd.concat([df, content])
                name = file.split('-')
                new_file_path = name[0] + '-' + name[1]
                df.to_csv('E:\\PycharmProjects\\ncDataProcess\\processed\\merge_files\\' + new_file_path)


merge_csv_file('processed')
