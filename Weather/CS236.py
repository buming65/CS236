from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import re
import os
import calendar
import time

def get_in_usa(path_location):
    spark_location = sc.textFile(path_location)
    in_usa = spark_location.map(lambda a: a.replace('\"', '').split(',')). \
        filter(lambda s: s[3] == 'US'). \
        filter(lambda s: s[4] != ''). \
        map(lambda s: (s[0], s[4]))
    return in_usa

def get_recording(path_recording):
    dir = os.listdir(path_recording)
    recordings = []
    for d in dir:
        path = args[2] + '/' +d
        print(path)
        recordings.append(sc.textFile(path))

    recording_all = sc.union(recordings)
    return recording_all

def get_min(recording, in_usa):
    # #Find the temperature for each day in each month of each station
    # first = record.map(lambda s: re.findall('[\S]+',s)).\
    #         map(lambda s: (s[0], (s[2][4:6], s[3])))
    # #join the stages
    # second = in_usa.join(first)
    # #keep stages months temperature
    # third = second.map(lambda s: ((s[1][0],s[1][1][0]),(float(s[1][1][1]))))
    # #find the min temperature for each month
    # fourth = third.reduceByKey(min)
    # #find the min temperature of the whole year for each stage
    # fifth = fourth.map(lambda x: (x[0][0], (x[0][1], x[1]))).\
    #                groupByKey().\
    #                map(lambda s: (s[0], sorted(s[1], key = lambda a:a[1])[0]))
    min_record_all = in_usa.join(recording.map(lambda s: re.findall('[\S]+', s)). \
                                 map(lambda s: (s[0], (s[2][4:6], s[3])))). \
        map(lambda s: ((s[1][0], s[1][1][0]), (float(s[1][1][1])))). \
        reduceByKey(min). \
        map(lambda x: (x[0][0], (x[0][1], x[1]))). \
        groupByKey(). \
        map(lambda s: (s[0], sorted(s[1], key=lambda a: a[1])[0]))
    return min_record_all

def get_max(recording, in_usa):
    max_record_all = in_usa.join(recording.map(lambda s: re.findall('[\S]+', s)). \
                                 map(lambda s: (s[0], (s[2][4:6], s[3])))). \
        map(lambda s: ((s[1][0], s[1][1][0]), (float(s[1][1][1])))). \
        reduceByKey(max). \
        map(lambda x: (x[0][0], (x[0][1], x[1]))). \
        groupByKey(). \
        map(lambda s: (s[0], sorted(s[1], key=lambda a: a[1])[-1]))
    return max_record_all

def calculate_difference(min_record, max_record):
    temp = max_record.join(min_record)
    # print(temp.collect())
    difference = temp.join(temp.map(lambda s: (s[0], round(s[1][0][1] - s[1][1][1], 1)))). \
        map(lambda s: (s[0], s[1][0][0][1], calendar.month_name[int(s[1][0][0][0])], s[1][0][1][1], calendar.month_name[int(s[1][0][1][0])], s[1][1])). \
        sortBy(lambda s: s[5])
    # print(difference.collect())

    return difference

#foreach is not working when ordering the difference
# def f(iterator):
#     for x in iterator:
#         string_result = x[0] + '\t' + str(x[1]) + '\t' + x[2] + '\t' + str(x[3]) + '\t' + x[4] + '\t' + str(x[5]) + '\r'
#         print(string_result)
#
#         with open(path_output + '/' + 'result.txt', mode='a') as file:
#             file.write(string_result)
#             file.close()


if __name__ == '__main__':
    start = time.time()
    args = sys.argv
    print(args)
    if len(args) != 4:
        print('ERROR: The length of configurations is not right.\n'
              'Please input the path of the Locations File, the Recordings Files and the Output Folder.\n'
              'EXITING')
        sys.exit(1)

    path_location = args[1]
    path_recording = args[2]
    # global path_output
    path_output = args[3]

    spark = SparkSession.builder.master("local").appName("SparkRDD")
    sc = SparkContext.getOrCreate()

    in_usa = get_in_usa(path_location)

    recording = get_recording(path_recording)

    min_record = get_min(recording, in_usa)
    max_record = get_max(recording, in_usa)

    # print(min_record.sortByKey().collect())
    # print(max_record.sortByKey().collect())

    result = calculate_difference(min_record, max_record)
    # print(result.sortByKey().collect())
    # print(result.collect())

    string_head = 'STATE' + '\t' + 'AVERAGE TEMPERATURE OF THE HIGHEST MONTH' + '\t' + 'HIGHEST MONTH NAME' + '\t' + 'AVERAGE TEMPERATURE OF THE LOWEST MONTH' + '\t' + 'LOWEST MONTH NAME' + '\t' + 'DIFFERENCE BETWEEN THESE TWO MONTHS' + '\r'

    with open(path_output + '/' + 'result.txt', 'w') as file:
        file.write(string_head)
        file.close()

    test = result.sortBy(lambda s: s[5]).collect()
    for x in test:
        string_result = x[0] + '\t' + str(x[1]) + '\t' + x[2] + '\t' + str(x[3]) + '\t' + x[4] + '\t' + str(x[5]) + '\r'
        print(string_result)

        with open(path_output + '/' + 'result.txt', mode='a') as file:
            file.write(string_result)
            file.close()

    end = time.time()

    print(end - start)


    # test.sortBy(lambda s: s[5]).foreachPartition(f)


