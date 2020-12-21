# 求车流量
def sum(x):
    count = 0
    for value in x[1]:
        count = count + value
    return (x[0], count)
