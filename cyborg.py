#assumptions
#---------------
#data quality is good
#file format is csv
#header is limited to one row

from pyspark import SparkContext, SparkConf 
from pyspark.sql import SQLContext 
from pyspark.sql.types import * 
import os,sys 
from datetime import date, time, datetime
from dateutil.parser import parse
from subprocess import call

def main():
    
    #filepath = sys.argv[1] #filename = input('Enter the path')
    #header = sys.argv[2]
    local_path='/home/nugantla/spark/nyctaxisub.csv'
    funpath = '/home/nugantla/spark/functions.py'
    filename = local_path.split('/')[-1]
    
    #file check
    if not os.path.exists(local_path):
        sys.exit('Local File not Found')
    if not os.path.exists(funpath):
        sys.exit('Functions file not found')
        
    #copying file from local directory to hdfs for spark
    #hdfs://in-air-bimap123.corp.capgemini.com:8020
    hdfs_path='/user/nugantla/' + filename
    
    #file replaces existing file
    call(['hadoop', 'fs', '-copyFromLocal', '-f', local_path, hdfs_path]) 
    
    #loading functions file
    call(['python', funpath])
    from functions import *
    
    #initialize
    sc = SparkContext("local", appName="Cyborg")
    sqlContext = SQLContext(sc)
    lines = sc.textFile(hdfs_path)
    
    #assuming only one line is header
    header = lines.first()
    data = lines.filter(lambda l: l not in header)
    data_sp = data.map(lambda x: x.split(','))


    # chacking only first row for datatype
    fields = [StructField(header.split(',')[i].strip('"').strip(' '), get_type(data_rs[i].strip('"').strip(' ')) , True) for i in range(0,len(header.split(','))-1)]
    #what if field_names contain spaces
    
    schema=StructType(fields)
    
# standard boiler plate syntax
if __name__ == '__main__':
  main()