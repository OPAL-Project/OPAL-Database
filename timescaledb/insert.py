from __future__ import print_function
import sys
import os
import csv
import gc
import time
import psycopg2

from os import listdir
from os.path import isfile, join
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

__version__ = '1.0'
__all__ = []
__author__ = ''


def connect():
    """
    Connects to the
    """
    connection = psycopg2.connect(database='opal', user="postgres", password="", host="127.0.0.1")
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return connection

#####################################
# main program                      #
#####################################

fileIDs = []
fileSizes = []

if __name__ == "__main__":

    data = sys.argv[1]
    bulk_size = int(sys.argv[2])
    if not os.path.isdir(data):
        print(
            "Invalid argument the first argument must be a directory. Usage: insert.py  <dataDirectory, string> <bulk_size, Integer>")
        exit(-1)

    if not os.path.exists(data):
        print("The path doesn't exists: " + data)
        exit(-1)

    dataFiles = [f for f in listdir(data) if isfile(join(data, f))]
    i = 0
    for dataFile in dataFiles:
        dataFileFullPath = join(data, dataFile)
        start_time = time.time()
        with open(dataFileFullPath, 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',')
            bulk = []
            with connect() as connection:
                connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                curs = connection.cursor()
                args_str = ""
                for row in csvreader:
                    i += 1
                    if i==1 or (i - 1) % bulk_size == 0:
                        args_str ="(TIMESTAMP \'{}\',\'{}\',\'{}\',{:d},\'{}\',{:d},\'{}\',{:d},{:d})"\
                            .format(row[6], row[0],row[1],int(row[2]),row[3],int(row[4]),row[5],int(1 if row[7] == '' else row[7]),int(row[8]))
                    else:
                        args_str += ",(TIMESTAMP \'{}\',\'{}\',\'{}\',{:d},\'{}\',{:d},\'{}\',{:d},{:d})"\
                            .format(row[6], row[0],row[1],int(row[2]),row[3],int(row[4]),row[5],int(1 if row[7] == '' else row[7]),int(row[8]))
                    if i % bulk_size == 0:
                        print(i)
                        # Bulk insert
                        curs.execute("INSERT INTO opal(event_time, interaction_type, interaction_direction, " 
                                     "emiter_country, emiter_id, receiver_country, receiver_id, duration, antenna_id) "
                                     "VALUES " + args_str)
                        args_str = ""
                        # We clear the memory
                        # gc.collect()

                if args_str != "":
                    # We insert the remaining
                    curs.execute("INSERT INTO opal(event_time, interaction_type, interaction_direction, emiter_country, "
                                 "emiter_id, receiver_country, receiver_id, duration, antenna_id) VALUES " + args_str)

                print("/***********************************************/")
                print("The file " + dataFile + " has been inserted")
                print("Number of lines inserted: " + str(i))
                print("/***********************************************/")

        elapsed_time = time.time() - start_time
        print("Elapsed time to insert: {} ".format(str(elapsed_time)))
