from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.rdd import RDD
from pyspark.files import SparkFiles
from pyspark.storagelevel import StorageLevel
from pyspark.accumulators import Accumulator, AccumulatorParam
from pyspark.broadcast import Broadcast
from pyspark.serializers import MarshalSerializer, PickleSerializer
from pyspark.status import *
from pyspark.profiler import Profiler, BasicProfiler


conf = SparkConf().setAppName("hotelhodsrrequest_parsing")
sc = SparkContext(conf = conf)
textFile = sc.textFile("/user/hive/warehouse/ehotel.db/hotelhodsrrequest/year=2016/month=01/day=*/*.gz")
#filter by PCC
PCCArray = textFile.map(lambda line: line.split("|")).filter(lambda line: line[4] == 'B7ZB')
  
  
#GroupBy Date
shopperdaycount = PCCArray.map(lambda line: (line[0].split(" ")[0], 1)).reduceByKey(lambda a, b: a+b)
shopperdaycount.saveAsTextFile('/user/sg952655/Totalshopperdaycount/')
shopperdaycounttxt = sc.textFile("/user/sg952655/Totalshopperdaycount/*")
preview0 = shopperdaycounttxt.collect()
    
  
#Duplicate record count with sessionid, transactionid, propertycode as unique key
filteredduplicaterecordcount = PCCArray.map(lambda line: (line[1]+" "+line[2]+" "+line[6], 1)).reduceByKey(lambda a, b: a+b).filter(lambda line: line[1] > 1)
Orderedfilteredduplicaterecordcount = filteredduplicaterecordcount.map(lambda line: (line[1], line[0]))
Orderedfilteredduplicaterecordcount.saveAsTextFile('/user/sg952655/TotalOrderedfilteredduplicaterecordcount/')
Orderedfilteredduplicaterecordcounttxt = sc.textFile("/user/sg952655/TotalOrderedfilteredduplicaterecordcount/*")
preview1 = Orderedfilteredduplicaterecordcounttxt.collect()
  

import re
  
#RequestText Parsing
rqtparsing = PCCArray.filter(lambda line: ((line[7] != ' ') and (re.search('[0-9]?[0-9][A-Za-z]{3}-[0-9]?[0-9][A-Za-z]?[A-Za-z]{2}[0-9]?[0-9]', line[7])))).map(lambda line: (line[0].split(" ")[0], line[1], line[2], line[6], (re.search('[0-9]?[0-9][A-Za-z]{3}-[0-9]?[0-9][A-Za-z]?[A-Za-z]{2}[0-9]?[0-9]', line[7])).group()))
rqtparsing.saveAsTextFile('/user/sg952655/Totalrqtparsing/')
rqtparsingtxt = sc.textFile("/user/sg952655/Totalrqtparsing/*")
preview2 = rqtparsingtxt.collect()
  
print("shopperdaycount list of tuples: ")
print(preview0)
  
print("Orderedfilteredduplicaterecordcount list of tuples: ")
print(preview1)
  
print("rqtparsing list of tuples: ")
print(preview2)

sc.stop()