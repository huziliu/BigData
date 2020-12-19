# Author:Liu Zichen
# function:用于绘制直方图

import findspark

findspark.init()
import pyspark as ps

from matplotlib import pyplot as plt

# 创建SparkConf对象
conf = ps.SparkConf().setMaster("local").setAppName("movies")

# 创建SparkContext对象
context = ps.SparkContext(conf=conf)

# 读取文件
movies = context.textFile("./dataset/movies.dat")

# 分割字符串,创建RDD
movies_RDD = movies.map(lambda row: row.split("::"))

# 截取movies中Genres与MovieID，并按类别分类
new_movies_RDD = movies_RDD.map(lambda x: (str(x[2]), int(x[0]))).groupByKey()

# 遍历RDD
# for line in new_movies_RDD.toLocalIterator():
#     print(line)

# 遍历RDD并统计类别以及对应的电影数量,存到一个字典中
output_dict = {}
for line in new_movies_RDD.toLocalIterator():
    genre_lst = line[0].split("|")
    length = len(line[1])
    for value in genre_lst:
        if value not in output_dict.keys():
            output_dict[value] = length
        else:
            output_dict[value] = output_dict[value] + length

# 打印字典
print(output_dict)

# 作直方图
plt.bar(output_dict.keys(), output_dict.values())
plt.show()
