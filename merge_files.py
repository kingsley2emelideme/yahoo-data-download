import pandas as pd
import glob
import os
from datetime import date


class MergeFiles:
    def __init__(self, file_path):
        self.file_path = file_path

    def combine_files(self):
        os.chdir(self.file_path)
        results = pd.DataFrame([])
        for counter, file in enumerate(glob.glob("*.csv")):
            named_file = pd.read_csv(file)
            results = results.append(named_file)
        path_to_combined_files = 'E:\Projects\yahoo-data-download\MergedFiles'
        results.drop(['Unnamed: 0'], axis=1, inplace=True)
        results.to_csv(
            os.path.join(path_to_combined_files, 'combined_files_' + date.today().strftime('%Y-%m-%d') + '.csv'),
            index=False)


if __name__ == '__main__':
    download_path = 'E:\Projects\yahoo-data-download\Data'
    merger = MergeFiles(download_path)
    merger.combine_files()