import findspark

findspark.init()
import pyspark as ps

from utils import c_avg

# 创建SparkConf对象
conf = ps.SparkConf().setMaster("local").setAppName("movies")

# 创建SparkContext对象
context = ps.SparkContext(conf=conf)

# 读取文件
ratings = context.textFile("./dataset/ratings.dat")
movies = context.textFile("./dataset/movies.dat")

# 分割字符串,创建RDD
ratings_RDD = ratings.map(lambda row: row.split("::"))
movies_RDD = movies.map(lambda row: row.split("::"))

# # 创建RDD
# ratingsRDD = parts_ratings.map(lambda p: ps.sql.Row(userID=int(p[0]), MovieID=int(p[1]),
#                                                     Rating=float(p[2]), TimeStamp=int(p[3])))
# moviesRDD = parts_movies.map(lambda p: ps.sql.Row(MovieID=int(p[0]), Title=str(p[1]),
#                                                   Genres=str(p[2])))

# 将RDD截取movieID与Rating部分，同时分组
new_ratings_RDD = ratings_RDD.map(lambda x: (int(x[1]), int(x[2]))).groupByKey()
# 截取movies中movieID与Title部分备用
new_movies_RDD = movies_RDD.map(lambda x: (int(x[0]), str(x[1])))

# 测试3280的出现次数
# tmp = new_ratings_RDD.filter(lambda x: x[0] == 3280)
# print(tmp.count())

# 计算平均值与标准差
avg_ratings_RDD = new_ratings_RDD.map(c_avg).sortBy(lambda x: x[1], ascending=False)

# 遍历
# for line in avg_ratings_RDD.toLocalIterator():
#     print(line)

# 与moviesRDD连接,并排序
avg_ratings_RDD_sort = avg_ratings_RDD.join(new_movies_RDD).sortBy(lambda x: x[1][0][0], ascending=False)

# 遍历连接后的RDD
# for line in avg_ratings_RDD_sort.toLocalIterator():
#     print(line)

# 取RDD前10个值
top_10_avg = avg_ratings_RDD_sort.take(10)

# 输出
print("平均得分前十的电影为：")
for value in top_10_avg:
    print("MovieID:", end='')
    print(value[0], end='')
    print("      ", end='')
    print("Title:", end='')
    print(value[1][1])

# 输出每部电影的平均得分及标准差,要有电影名称，不含年
print()
print("每部电影的名称，平均得分以及标准差：")
for line in avg_ratings_RDD_sort.toLocalIterator():
    print("Title:", end='')
    lst = line[1][1].split(' ')
    output = ''.join(lst[:len(lst) - 1])
    print(output, end='')
    print("      ", end='')
    print("avg:", end='')
    print(line[1][0][0], end='')
    print("      ", end='')
    print("Standard Deviation:", end='')
    print(line[1][0][1])
