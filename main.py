import pandas as pd
import numpy as np
import os
import argparse
import glob


class ETL:

    # requirement1:Remove all empty lines
    @staticmethod
    def remove_empty_lines():
        if os.path.exists("req1.csv"):
            os.remove("req1.csv")
        else:
            df = pd.read_csv('datafile.csv')
            df.dropna(how="all", inplace=True)
            df.to_csv("req1.csv", index=False)
            print("Done")

    # requirement2:Replace Empty value(Empty Cell) With NA
    @staticmethod
    def replace_empty_values():
        if os.path.exists("req2.csv"):
            os.remove("req2.csv")
        else:
            df = pd.read_csv('req1.csv')
            replaced_data = df.replace(np.nan, "NA")
            print(replaced_data)
            replaced_data.to_csv("req2.csv", index=False)
            print("Done")

    # requirement3:Delete all duplicate row
    @staticmethod
    def remove_duplicate_row_values():
        if os.path.exists("req3.csv"):
            os.remove("req3.csv")
        else:
            df = pd.read_csv('req2.csv')
            print(",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,")
            size = len(df)
            print("Print Length Of Rows Before Remove Duplicate: {}".format(size))
            remove_dup_data = df.drop_duplicates(subset="PG", keep=False, inplace=True)
            df.to_csv("req3.csv", index=False)
            size = len(df)
            print("Print Length Of Rows After Remove Duplicate: {}".format(size))

    # requirement4:main function of all assignment split one excel sheet into multi sheet depend on input User
    @staticmethod
    def split_colunm(no):
        if os.path.exists("file*.csv"):
            fileList = glob.glob('file*.csv')
            for filePath in fileList:
                try:
                    os.remove(filePath)
                except:
                    print("Error while deleting file : ", filePath)
        else:
            if os.path.exists("req3.csv"):
                df = pd.read_csv('req3.csv')
                column = df.columns.values
                sel_col = column[no]
                unique_data = df[sel_col].unique()
                for state in unique_data:
                    print(state)
                    new_df = df[df[sel_col] == state]
                    print(new_df)
                    new_df.to_csv("file_" + str(state) + ".csv")


    # additional requirement
    @staticmethod
    def sort_data():
        df = pd.read_csv('datafile.csv')
        df.sort_values("PG", inplace=True)
        print("Print Sorted Excel Depend on PG")
        print(df)

    # show first 5 rows in Excel Sheet
    @staticmethod
    def first_five_rows():
        df = pd.read_csv('datafile.csv')
        print("Print First Five Rows: {}".format(df.head()))

    # show Last 5 rows in Excel Sheet
    @staticmethod
    def last_five_rows():
        df = pd.read_csv('datafile.csv')
        print("Print Last Five Rows: {}".format(df.tail()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Split Column Depend On User Input')
    parser.add_argument('-column', '--numberCol', type=int, help='Select Column Number')
    args = parser.parse_args()
    print(args.numberCol)
    b = args.numberCol
    if 0 <= b <= 4:
        ETL.remove_empty_lines()
        ETL.replace_empty_values()
        ETL.remove_duplicate_row_values()
        ETL.split_colunm(args.numberCol)
    else:
        print("Out of the index")
