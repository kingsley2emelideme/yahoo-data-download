import pandas as pd
import glob
import os


class MergeFiles:
    def __init__(self, file_path):
        self.file_path = file_path

    def combine_files(self):
        os.chdir(self.file_path)
        for counter, file in enumerate(glob.glob("*.csv")):
            named_file = pd.read_csv(file)
            path_to_combined_files = os.path.join('E:\Projects\yahoo-data-download\MergedFiles', file)
            print('Reformatting file:', os.path.join('E:\Projects\yahoo-data-download\MergedFiles', file))
            named_file.drop(['Unnamed: 0'], axis=1, inplace=True)
            named_file.to_csv(path_to_combined_files, index=False)


m = MergeFiles('E:\Projects\yahoo-data-download\Data')
m.combine_files()

