#Krishnan We_43
from pyspark.sql.session import SparkSession
spark=SparkSession.builder.master('local').appName("Krishnan").config("spark.executor.cores", "2").enableHiveSupport().getOrCreate()
izsc=spark.sparkContext

# hdfs_file_rdd=izsc.textFile('hdfs:///user/hduser/custo')
# print(hdfs_file_rdd.count())

# hdfs_file_rdd=izsc.textFile('hdfs:///user/hduser/videolog/youtube_videos.tsv')
# print('All data \n',hdfs_file_rdd.collect())
# print('All data count \n',hdfs_file_rdd.count())
# func=lambda x: 'Travel' in x or 'Python' in x
# filtered_rdd=hdfs_file_rdd.filter(func)
# print('Filtered rdd  count ' ,filtered_rdd.count())
# print('Filtered rdd ' ,filtered_rdd.collect())
# hdfs_file_rdd.cache()
# print('All data count \n',hdfs_file_rdd.count())
# hdfs_file_rdd.cache()

# lst=[('4000001,Kristina,Chung,55,Pilot'),
#      ('000002,Paige,Chen,77,Teacher'),
#      ('000002,Paige,Chen,77,Teacher')]
#
# rdd_lst=izsc.parallelize((lst,3))
# print(rdd_lst.glom().collect())
#
# lst=range(1,100)
# rdd1=izsc.parallelize(lst,10)
# print(rdd1.glom().collect())

#usecase

# rdd1=izsc.textFile('File:///home/hduser/custo')
# print(rdd1.collect())
# rdd2=rdd1.map(lambda x:x.split(','))
# print(rdd2.collect())
# func=lambda x : (int(x[0]),str(x[1]),int(x[3]))
# rdd3=rdd2.map(func)
# print(rdd3.collect())


# rdd1=izsc.textFile('File:///home/hduser/custo')
# print(rdd1.take(4))
# rdd2=rdd1.filter(lambda x : 'Teacher' in x or 'fighter' in x)
# print(rdd2.collect())

# lines = izsc.textFile('File:///home/hduser/custo')
# print(lines.collect())
# lengths = lines.map(lambda l: len(l))
# print(lengths.collect())

# lengths = lines.map(lambda x: x.split(",")).map(lambda l: len(l))
# print(lines.collect())
# print(lengths.collect())

# sal_lst=[200000,500000,450000,570000]
# rdd=izsc.parallelize(sal_lst)
# print(rdd.collect())
# rdd1=rdd.filter(lambda x :x>=450000)
# print(rdd1.collect())

# rdd1=izsc.textFile('File:///home/hduser/custo')
# print(rdd1.collect())
# rdd2=rdd1.map(lambda x:x.split(','))
# print(rdd2.collect())
# rdd3=rdd2.filter(lambda x:int(x[3])>50)
# print(rdd3.collect())
# print(rdd3.count())

# rdd1=izsc.textFile('File:///home/hduser/custo')
# print(rdd1.collect())
# rdd2=rdd1.map(lambda x:x.strip().split(','))
# print(rdd2.collect())
# rdd3=rdd2.filter(lambda x : len(x) in [2,3,4])
# print(rdd3.collect())

# rdd1=izsc.textFile('File:///home/hduser/custo')
# print(rdd1.collect())
# rdd2=rdd1.map(lambda x: len(x))
# print(rdd2.collect())

# data=['id,name,age',
#        '1,irfan,42',
#          '2,aaa,30']
# rdd1=izsc.parallelize(data)
# print(rdd1.collect())
# header=rdd1.first()
# print(header)
# data_no_header = rdd1.filter(lambda row: row != header)
# print(data_no_header.take(5))

# header = data.first()
#
# # Step 2: Filter out the header from the RDD
# data_no_header = data.filter(lambda row: row != header)
#
# # Step 3 (Optional): Use take() to view the resulting dataset without the header
# print(data_no_header.take(5))


#Define spark session object / sc
#Create an RDD from the file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log
#Apply transformation to filter only ERROR and WARN data from the above rdd
#Store the error data in /user/hduser/errdata/ and warning data in /user/hduser/warndata/

# rdd1=izsc.textFile('file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log')
# print(rdd1.take(5))
# err_rdd=rdd1.filter(lambda x : 'ERROR' in x or 'Error' in x or 'error' in x)
# print(err_rdd.collect())
# warn_rdd=rdd1.filter(lambda x : 'WARN' in x or 'Warn' in x or 'warn' in x)
# print(warn_rdd.count())
# err_rdd.saveAsTextFile('hdfs:///user/hduser/errdata_1')
# warn_rdd.saveAsTextFile('hdfs:///user/hduser/warndata_1')


# file_rdd1=izsc.textFile('file:///usr/local/hadoop/logs/hadoop-hduser-namenode-localhost.localdomain.log')
# filtered_rdd=file_rdd1.filter(lambda x:len(x)>=2)
# err_rdd=filtered_rdd.filter(lambda x:x[2]=='ERROR')
# warn_rdd=filtered_rdd.filter(lambda x:x[2]=='WARN')
# err_rdd.saveAsTextFile("hdfs:///user/hduser/errdata_i")
# warn_rdd.saveAsTextFile("hdfs:///user/hduser/warndata_i")

# rdd= izsc.textFile('hdfs:///user/hduser/errdata_1/part-00000')
# print(rdd.collect())

rdd=izsc.textFile('file:///home/hduser/custo').map(lambda x : x.split(','))
fil_rdd=rdd.filter(lambda x : len(x)==5)
print(fil_rdd.count())
trans_rdd = fil_rdd.map(lambda x : x[4])
transa_rdd = fil_rdd.map(lambda x : (int(x[0]),(x[1],x[2])))#transa_rdd = fil_rdd.map(lambda x: (int(x[0]), x[1], x[2]))
# print(trans_rdd.distinct().collect())
print(trans_rdd.countByKey())
print(trans_rdd.countByValue())
print(transa_rdd.lookup(4000001))

#EOF









