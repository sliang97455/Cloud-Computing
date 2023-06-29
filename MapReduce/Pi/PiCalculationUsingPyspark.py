from pyspark import SparkContext, SparkConf

def process_line(line):
    line = line.replace("(", "").replace(")", "").replace(",", " ")
    tokens = line.split()
    radius = 200
    x_value = int(tokens[0])
    y_value = int(tokens[1]) if len(tokens) > 1 else 0
    check = ((radius - x_value) ** 2 + (radius - y_value) ** 2) ** 0.5
    return ("inside" if check < radius else "outside", 1)

if main():
    conf = SparkConf().setAppName("PiCalculation")
    sc = SparkContext(conf=conf)
    text_file = sc.textFile("/path/to/input.txt")
    points = text_file.map(process_line)
    counts = points.reduceByKey(lambda a, b: a + b)
    
    inside = counts.lookup("inside")[0]
    outside = counts.lookup("outside")[0]
    pi = 4.0 * inside / (inside + outside)

    print("PI: ", pi)

main()
