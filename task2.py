#Task2: Exploration on Multiple Datasets
import sys
import json
from collections import defaultdict
from pyspark import SparkContext,SparkConf
con = SparkConf().setAll([('spark.executor.memory', '8g'), ('master','local'),('appName','task2'),('spark.driver.memory','8g')])
sc = SparkContext(conf=con)
######################################################

if len(sys.argv) != 6:
    print("Supply enough arguments")
    exit(-1)
else:
    reviewsfile_path = sys.argv[1]
    businessfile_path = sys.argv[2]
    outputfile_path = sys.argv[3]
    if_spark = sys.argv[4]
    n=int(sys.argv[5])


#run below code for spark version
if if_spark=="spark":
    Top_categories = {}
    # initializing RDDs
    reviewRdd = sc.textFile(reviewsfile_path).map(lambda x1: json.loads(x1)) \
        .map(lambda a: (a['business_id'], a['stars'])).filter(lambda x: x != "")
    businessRdd = sc.textFile(businessfile_path).map(lambda x2: json.loads(x2)) \
        .map(lambda a: (a['business_id'], a['categories'])).filter(lambda x: x != "")


    def Func(pair):
        m = []
        if pair[0] is not None:
            if ',' in pair[0]:
                lines = pair[0].split(",")
                for l in lines:
                    m.append([l.strip(), pair[1]])
            else:
                m.append((pair[0], pair[1]))
        return m


    final0Rdd = reviewRdd.join(businessRdd) \
        .map(lambda key_value: (key_value[1][1], key_value[1][0]))
    final1Rdd = final0Rdd.flatMap(Func)
    f = final1Rdd.mapValues(lambda value: (value, 1))
    f1 = f.reduceByKey(lambda x1, y1: (x1[0] + y1[0], x1[1] + y1[1])).mapValues(lambda value: value[0] / value[1]) \
        .sortBy(lambda e: (-e[1], e[0]))

    A = f1.take(int(n))
    Top_categories["result"] = A

else:
    from collections import defaultdict
    with open(businessfile_path,'r') as myfile1:
        data1=myfile1.readlines()
    with open(reviewsfile_path,'r') as myfile2:
        data2=myfile2.readlines()

    id_stars=[]
    id_categories=[]
    answer=[]
    id_stars_dict=defaultdict(list)
    id_categories_dict=defaultdict(list)
    average_stars_dict={}


    for val in data1:
        i = json.loads(val)
        if i['categories'] is not None:
            cat = i['categories'].strip()
            for c in cat.split(','):
                c = str(c)
                id_categories.append((i['business_id'], str(c.strip())))

    for val in data2:
        j=json.loads(val)
        id_stars.append((j['business_id'].strip(),j['stars']))

    for id,stars in id_stars:
        id_stars_dict[id].append(stars)

    for id,category in id_categories:
        if id in id_stars_dict:
            for st in id_stars_dict[id]:
                id_categories_dict[category].append(st)

    for a,b in id_categories_dict.items():
        average_stars_dict[a]=sum(b)/len(b)
    answer = sorted(average_stars_dict.items(), key=lambda v1: v1[0])
    final_answer = sorted(answer, key=lambda v1: v1[1], reverse=True)

    A = final_answer[0:n]
    Top_categories = {}
    Top_categories["result"] = A
with open(outputfile_path, 'w+') as rfp:
    json.dump(Top_categories, rfp)

print("end")
#$ spark-submit task2.py <review_file> <business_file > <output_file> <if_spark> <n> /asnlib/publicdata

#$ spark-submit task2.py <review_file> <business_file > <output_file> <if_spark> <n> /asnlib/publicdata

