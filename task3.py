from time import time
from pyspark import SparkConf, SparkContext,StorageLevel
import sys
import json
con = SparkConf().setAll([('spark.executor.memory', '8g'), ('master','local'),('appName','new'),('spark.driver.memory','8g')])
sc = SparkContext(conf=con)

'''
if len(sys.argv)!=6:
    print("Not Enough arguments supplied")
    exit(-1)
else:
    
    reviewdatapath = sys.argv[1]
    outputdatapath = sys.argv[2]
    partition_type = sys.argv[3]
    num_partitions = int(sys.argv[4])
    n = int(sys.argv[5])
'''
reviewdatapath = r"D:\DM sem2\Assignements\A1 Data\review.json"
outputdatapath = r"D:\DM sem2\Assignements\A1 Data\output_task3_default"
partition_type = "default"
num_partitions = 5
n = 5
out = {}
read_json = sc.textFile(reviewdatapath)
def customizePartitioner(business_id):
    return hash(business_id)%num_partitions
if partition_type =='customized':
    
    rdd=read_json.map(json.loads).map(lambda r:(r['user_id'],r['review_id'],r['business_id'],r['date'])).persist()
    rdd2 = rdd.map(lambda r:(r[2],1)).partitionBy(num_partitions, customizePartitioner).reduceByKey(lambda a,b: a+b).filter(lambda v:v[1] > n)
    no_of_items = rdd2.glom().map(len).collect()
    out['n_items'] = no_of_items
    out['result'] = rdd2.collect()
    out['n_partitions'] = num_partitions
   
else:
    
    rdd=read_json.map(json.loads).map(lambda r:(r['user_id'],r['review_id'],r['business_id'],r['date'])).persist()
    n_reviews = rdd.map(lambda r:(r[2],1)).reduceByKey(lambda a,b: a+b).filter(lambda v:int(v[1]) > n)
    no_of_items = n_reviews.glom().map(len).collect()
    m=n_reviews.getNumPartitions()
 
    out["result"]=n_reviews.collect()
    out["n_items"]=no_of_items
    out["n_partitions"]=m 
   

with open(outputdatapath, 'w+') as finalout:
    json.dump(out, finalout)