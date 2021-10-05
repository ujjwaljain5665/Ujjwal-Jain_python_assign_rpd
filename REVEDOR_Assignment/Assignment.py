# importing modules
import pandas as pd
import matplotlib.pyplot as mp
import seaborn as sb
import pymysql

class task:

    def csv_to_fromattedcsv(self):                                                             # function to read csv file and write into processed csv

        data=pd.read_csv('HDFCBANK_01-01-2021_27-09-2021.csv',
        usecols=['Date','High Price','Low Price','Total Traded Quantity',
        'Turnover (in lacs)','No. of Contracts'])                                              # reading the csv file by specifying the particular column to read

        value=['date','high','low','trade','turnover_in_lakhs','contracts']                    # specifying the new column name

        data.columns=[value]                                                                   # renaming the columns

        data['difference'] =data.apply(lambda x: x['high'] - x['low'], axis = 1)               # adding a new column 

        data.to_csv("processed_data.csv",index=False)                                          # writing the data into processed csv
    

    def csv_to_sql(self):                                                                                       # function to write the data from processed csv into mysql.

        data=pd.read_csv('processed_data.csv')                                                                  # reading csv file
        try:
            con=pymysql.connect(host='localhost',user='root',
            password='',db='python_fin_test')                                                                   # connecting to database

            cur=con.cursor()                                                                                    # creating a cursor

            cols = "`,`".join([str(i) for i in data.columns.tolist()])                                          # creating column list for insertion

            for i,row in data.iterrows():                                                                       # inserting data records one by one.

                sql = "INSERT INTO `processed_data` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"      # sql query

                cur.execute(sql, tuple(row))                                                                    # executing the sql query and passing the values

                con.commit()                                                                                    # the connection is not autocommitted by default, so we must commit to save our changes
        except Exception as e:
            print("Error due to -"+str(e))

    def correltn_mat(self):                                                     # function for creating correlation matrix.

        data=pd.read_csv('processed_data.csv')                                  # reading the csv file

        # print(data.corr())                                                      # if we want to see the data to be plotted

        dataplot = sb.heatmap(data.corr(), cmap="YlGnBu", annot=True)           # plotting correlation heatmap

        mp.show()                                                               # displaying heatmap

if __name__=="__main__":
    obj=task()                  # creating object of the class

    obj.csv_to_fromattedcsv()   # calling the function to read the csv and writing into processed csv
    
    obj.csv_to_sql()            # uploading the data from processed csv to mysql
    
    obj.correltn_mat()          # creating correlation matrix