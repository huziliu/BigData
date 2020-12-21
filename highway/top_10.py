import findspark

findspark.init()

import pyspark as ps

from utils import sum

# 创建SparkConf对象
conf = ps.SparkConf().setMaster("local").setAppName("movies")

# 创建SparkContext对象
context = ps.SparkContext(conf=conf)

# 读取文件
highway_RDD = context.textFile("./dataset/201601.tab", 5)
station_RDD = context.textFile("./dataset/收费站信息数据.tab")

# 分割字符串
parts_highway_RDD = highway_RDD.map(lambda x: x.split('\t'))
parts_station_RDD = station_RDD.map(lambda x: x.split('\t'))

# 去除属性名
parts_highway_RDD = parts_highway_RDD.filter(lambda x: x[0] != '入口网络编号')
parts_station_RDD = parts_station_RDD.filter(lambda x: x[0] != 'id')

# 获取进站信息
in_highway_RDD = parts_highway_RDD.map(lambda x: (float(x[1]), str(x[21])))

# 获取出站信息
out_highway_RDD = parts_highway_RDD.map(lambda x: (float(x[4]), str(x[21])))

# 获取处理后的收费站信息,并去除名字为未知以及未开通的区域
new_station_RDD = parts_station_RDD.map(lambda x: (float(x[1]) * 256 + float(x[2]), str(x[4]))).filter(
    lambda x: x[1] != '"未知"' and x[1] != '"未开通"')

# # 连接,并处理新得到的RDD
# in_count = in_highway_RDD.join(new_station_RDD).groupByKey().join(new_station_RDD)
# out_count = out_highway_RDD.join(new_station_RDD).groupByKey().join(new_station_RDD)
#
# # 计算车流量
# new_in_count = in_count.map(lambda x: (x[1][1], len(x[1][0]))).groupByKey()
# sum_in_count = new_in_count.map(sum)
# new_out_count = out_count.map(lambda x: (x[1][1], len(x[1][0]))).groupByKey()
# sum_out_count = new_out_count.map(sum)

# 连接,并处理新得到的RDD
in_count = in_highway_RDD.join(new_station_RDD)
out_count = out_highway_RDD.join(new_station_RDD)
new_in_count = in_count.map(lambda x: (x[1][1], x[1][0])).groupByKey()
new_out_count = out_count.map(lambda x: (x[1][1], x[1][0])).groupByKey()

# 计算车流量
sum_in_count = new_in_count.map(lambda x: (x[0], len(x[1])))
sum_out_count = new_out_count.map(lambda x: (x[0], len(x[1])))
sum_RDD = sum_in_count.fullOuterJoin(sum_out_count)
sum_RDD = sum_RDD.map(lambda x: (x[0], x[1][0] + x[1][1]))

# 排序,取Top10
sum_RDD = sum_RDD.sortBy(lambda x: x[1], ascending=False)

# for line in sum_RDD.toLocalIterator():
#     print(line)

Top_10_RDD = sum_RDD.take(10)

# 遍历Top10
for line in Top_10_RDD:
    print(line)
