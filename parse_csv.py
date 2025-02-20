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
# sc.textFile(path/to/file.csv) to read the csv file
lines = sc.textFile("./data/exam_results.csv")
# map to esolate each comma separated value to its own field
parsed_lines = lines.map(parseLine)
# collect to actually carry out the work so the results can be produced to be printed
results = parsed_lines.collect()

for row in results:
    print(row)