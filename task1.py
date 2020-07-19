from time import time
from pyspark import SparkConf, SparkContext,StorageLevel
import sys
import json
con = SparkConf().setAll([('spark.executor.memory', '8g'), ('master','local'),('appName','task1'),('spark.driver.memory','8g')])
sc = SparkContext(conf=con)

if len(sys.argv) != 7:
    print("Input in form: $ spark-submit task1.py <input_file> <output_file> <stopwords> <y> <m> <n>")
    exit(-1)
else:
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    stopwordsFile=sys.argv[3]
    y=sys.argv[4]
    m=sys.argv[5]
    n=sys.argv[6]
final_dic={}
'''
#sample input
inputFile=r"D:\DM sem2\Assignements\A1 Data\review.json"
outputFile=r"D:\DM sem2\Assignements\A1 Data\output_task1"
stopwordsFile=r"D:\DM sem2\Assignements\A1 Data\stopwords"
y="2010"
m="5"
n="5"
'''
#json.loads as a function to apply to each line of text file to convert string to a python dictionary
reviewRdd = sc.textFile(inputFile).map(json.loads)

reviewRdd=reviewRdd.map(lambda e : (( e['user_id'],e['review_id'],e['date'], e['business_id'],e['text'] ), 1)).persist(storageLevel=StorageLevel.MEMORY_ONLY)

#1.1 to count the number of reviews
reviewno = reviewRdd.map(lambda e: e[0][1]).count()
final_dic["A"] = reviewno
print(reviewno)
#1.2 No. of reviews in a given year y

reviews_in_y_count = reviewRdd.filter(lambda e: y in e[0][2])
reviews_in_y = reviews_in_y_count.count()
final_dic["B"] = reviews_in_y

#1.3 No. of distinct users
distinct_user=reviewRdd.map(lambda e: e[0][0]).distinct()
d_user=distinct_user.count()
final_dic["C"]=d_user

#1.4 Top m users having largest no. of reviews and its count
count2 = reviewRdd.map(lambda e: (e[0][0],1)).reduceByKey(lambda x,y: x+y)
count1=count2.sortBy(lambda e2: (-e2[1], e2[0]))\
    .take(int(m))
final_dic["D"]=count1

#1.E. Top n frequent words in the review text. The words should be in lower cases.
def Func(lines):
    lines = lines.lower()
    for c in ["(", "[", ",",".", "!", "?", ":", ";", "]", ")"]:
        lines=lines.replace(c,"")
    lines = lines.split()
    return lines

reviewtext=reviewRdd.map(lambda e: (e[0][4]))
reviewtext2=reviewtext.flatMap(Func)
with open(stopwordsFile) as f:
    lines = [line.rstrip() for line in f]

eliminate=["(", "[", ",",".", "!", "?", ":", ";", "]", ")"] + lines
rdd3 = reviewtext2.filter(lambda x: x not in eliminate)

#converting in k,v pair to apply transformation and action
rdd3_mapped = rdd3.map(lambda x: (x,1))
prefinal=rdd3_mapped.reduceByKey(lambda x,y: x+y)
final=prefinal.map(lambda x:(x[1],x[0])).sortByKey(False)\
    .map(lambda x:(x[1]))\
    .take(int(n))
print(final)
final_dic["E"]=final

with open(outputFile, 'w+') as rfp:
    json.dump(final_dic, rfp)