import json
import sys
import time

from operator import add
from pyspark import SparkContext
from pyspark import SparkConf

# You will need to compute businesses that have more than n reviews in the review file
# customized partition function

# no of partitions of the RDD, number of items per partition, business that have more than n reviews

def custom_hash(value):
	return hash(value)

if __name__ == '__main__':

	input_file = sys.argv[1]
	output_file = sys.argv[2]
	partition_type = sys.argv[3]
	n_partitions = int(sys.argv[4])
	n = int(sys.argv[5])

	result = dict()

	sc = SparkContext(appName="DSCI553TASK3", master="local[*]")

	sc_conf = SparkConf().setAppName("DSCI553HW1TASK3").setMaster("local[*]")

	sc = SparkContext.getOrCreate(conf=sc_conf)

	review_file_ip = sc.textFile(input_file).map(lambda row: json.loads(row))

	business_id_rdd = review_file_ip.map(lambda kv:(kv['business_id'], 1))

	if partition_type != "default":
		business_id_rdd = business_id_rdd.partitionBy(numPartitions=n_partitions, partitionFunc=custom_hash)

	result["n_partitions"] = business_id_rdd.getNumPartitions()

	result["n_items"] = business_id_rdd.glom().map(len).collect()

	result["result"] = business_id_rdd.reduceByKey(add).filter(lambda kv: kv[1] > n).collect()

	with open(output_file, "w") as fp:
		json.dump(result, fp)
