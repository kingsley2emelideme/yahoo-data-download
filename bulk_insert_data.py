import pyodbc
import glob
import os


def bulk_insert_data():
    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=KINGSLEY;DATABASE=Security; UID=kings;\
    Trusted_Connection=yes;')
    cursor = conn.cursor()
    os.chdir('E:\Projects\yahoo-data-download\MergedFiles')
    for ff in glob.glob('*.csv'):
        from_path = os.path.join('E:\Projects\yahoo-data-download\MergedFiles', ff)
        print('Bulk inserting file: ', os.path.join('E:\Projects\yahoo-data-download\MergedFiles', ff))
        query = "BULK INSERT [Security].[dbo].[SecurityPrice] FROM '{0}' WITH (FIRSTROW = 2, FIELDTERMINATOR = ','," \
                "ROWTERMINATOR = '\n');".format(from_path)
        try:
            cursor.execute(query)
            conn.commit()
        except Exception as e:
            print(e, ff)
            continue
    conn.close()
    return