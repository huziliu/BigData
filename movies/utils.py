# 定义用于计算平均值与标准差的函数
def c_avg(x):
    num = 0
    sum = 0
    for value in x[1]:
        sum = sum + value
        num = num + 1
    avg = sum / num
    sum = 0
    for value in x[1]:
        sum = sum + (value - avg) ** 2
    sd = (sum / num) ** 0.5
    return (x[0], (avg, sd))


# 定义用于统计男女数量的函数
def count(x):
    male_count = 0
    female_count = 0
    for value in x[1]:
        if value == 'M':
            male_count = male_count + 1
        else:
            female_count = female_count + 1
    return (x[0], male_count, female_count)
