#Author: Abhijith Umesh
#DSCI-553, Task-2
import sys
import json

from pyspark import SparkContext
from pyspark import SparkConf


if __name__ == "__main__":

	review_file = sys.argv[1]
	business_file = sys.argv[2]
	output_file = sys.argv[3]

	if_spark = sys.argv[4]
	n_value = sys.argv[5]

	n_value = int(n_value)

	res = dict()

	if if_spark == "spark":

		sc = SparkContext(appName="DSCI553TASK2", master="local[*]")

		sc_conf = SparkConf().setAppName("DSCI553HW1TASK2").setMaster("local[*]")

		sc = SparkContext.getOrCreate(conf=sc_conf)

		business_lines = sc.textFile(business_file).map(lambda row:json.loads(row))

		review_lines = sc.textFile(review_file).map(lambda row:json.loads(row))

		top_n_categories = \
						business_lines.filter(lambda line: line['categories'] is not None).\
						map(lambda line: (line['business_id'], line['categories'])).\
						join(review_lines.map(lambda line:(line['business_id'], line['stars']))). \
						flatMap(lambda line:[(category.strip(),(line[1][1],1)) for category in line[1][0].split(",")]). \
						reduceByKey(lambda a,b: (a[0] + b[0], a[1]+b[1])). \
						mapValues(lambda a: float(a[0]/a[1])). \
						takeOrdered(n_value, key=lambda kv:(-kv[1], kv[0]))


		top_n_categories = [list(val) for val in top_n_categories]

		res['result'] = top_n_categories

	elif if_spark == "no_spark":

		business_stars = {}
		business_category = {}


		with open(review_file, "r") as fp:
			
			for line in fp:
				
				json_object = json.loads(line)
				json_object = (json_object['business_id'], json_object['stars'])

				if(json_object[0] in business_stars):
				
					sum_value, count = business_stars[json_object[0]]

					sum_value += json_object[1]
					count += 1
					business_stars[json_object[0]] = (sum_value, count)

				else:

					business_stars[json_object[0]] = (json_object[1], 1)


		with open(business_file, "r") as fp:

			for line in fp:
				json_object = json.loads(line)
				json_object = (json_object['business_id'], json_object['categories'])

				if json_object[1] is not None and json_object[1] != "":

					category_lis = json_object[1]
					category_lis = [value.strip() for value in category_lis.split(',')]
					category_lis = set(category_lis)

					if json_object[0] in business_category:

						value = business_category[json_object[0]]
						value.update(category_lis)

						business_category[json_object[0]] = value

					else:
						business_category[json_object[0]] = category_lis 


		joined_dict = dict()

		for buss_id, buss_score in business_stars.items():

			if buss_id in business_category :

				for category in business_category[buss_id]:

					if category not in joined_dict:
						joined_dict[category] = buss_score

					else:
						value = joined_dict[category][0] + buss_score[0]
						count = joined_dict[category][1] + buss_score[1]

						joined_dict[category] = (value, count)

		joined_dict = {k: float(v[0]/v[1]) for k,v in joined_dict.items()}


		res['result'] = sorted(joined_dict.items(), key=lambda kv:(-kv[1],kv[0])) [:n_value]



	with open(output_file, "w") as fp:
		json.dump(res, fp)


