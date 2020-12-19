# Author:Liu Zichen
# function:用于绘制饼状图

import findspark

findspark.init()
import pyspark as ps

from utils import count

from matplotlib import pyplot as plt

# 创建SparkConf对象
conf = ps.SparkConf().setMaster("local").setAppName("movies")

# 创建SparkContext对象
context = ps.SparkContext(conf=conf)

# 读取文件
users = context.textFile("./dataset/users.dat")

# 分割字符串,创建RDD
users_RDD = users.map(lambda row: row.split("::"))

# 提取users中的Occupation（1，6，16）与Gender,并按Occupation进行分类
new_users_RDD = users_RDD.map(lambda x: (int(x[3]), str(x[1]))).filter(
    lambda x: x[0] == 1 or x[0] == 6 or x[0] == 16).groupByKey()

# 遍历RDD
# for line in new_users_RDD.toLocalIterator():
#     print(line)

# 对RDD中的每一组数据进行处理，统计各自男女数量并生成新的RDD
users_RDD_count = new_users_RDD.map(count)

# 遍历RDD并做出相应的饼状图
for line in users_RDD_count.toLocalIterator():
    plt.title(line[0])
    size = line[1:]
    labels = ["Male", "Female"]
    plt.pie(size, labels=labels)
    plt.show()
