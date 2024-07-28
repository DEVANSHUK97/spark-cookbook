import findspark
findspark.init()
from pyspark import SparkConf, SparkContext

conf = SparkConf().set("spark.master", "local").setAppName("parse_csv")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    field_name_1 = fields[0]
    field_name_2 = fields[1]
    field_name_3 = fields[2]
    field_name_4 = fields[3]
    return (field_name_1, field_name_2, field_name_3, field_name_4)

lines = sc.textFile("./data/exam_results.csv")
parsed_lines = lines.map(parseLine)

filtered_lines = parsed_lines.filter(lambda x: x[3] == 'A')

results = filtered_lines.collect()
for row in results:
    print(row)