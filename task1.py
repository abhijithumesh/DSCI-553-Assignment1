# Author: Abhijith Umesh
# USC - Data Mining Task1

import sys
import json
import re

from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

# The total number of reviews
def total_num_reviews(review_ids):

	return review_ids.count()

# The number of reviews in a given year, y
def number_of_reviews_y(review_years, year):

	review_filter = review_years.filter(lambda x:re.match("^\d{4}",x).group(0) == year).count()
	return review_filter

def number_of_distinct_business(business_ids_rdd):

	return business_ids_rdd.distinct().count()

def top_m_users(business_user_rdd, m_users):

	return business_user_rdd.map(lambda kv:(kv[1], 1)).reduceByKey(add). \
		takeOrdered(m_users, key=lambda kv: (-kv[1], kv[0]))

def remove_punctuation(text):

	text = text.lower().strip()

	for punct in exclude_set:
		text = text.replace(punct, "")

	return text

def most_repeated_words(review_text_rdd, top_n):

	words_dict = review_text_rdd.flatMap(lambda x:remove_punctuation(x[1]).split(" ")). \
				 filter(lambda word: word not in stopword_list and word is not None and word != " " and word != ""). \
				  map(lambda word:(word, 1)).reduceByKey(add).takeOrdered(top_n, key = lambda kv: (-kv[1], kv[0]))

	subres = [word_count[0] for word_count in words_dict]
	return subres


if __name__ == "__main__":

	input_file = sys.argv[1]
	output_file = sys.argv[2]
	stopword_file = sys.argv[3]
	year = sys.argv[4]
	m_users = sys.argv[5]
	n_freq = sys.argv[6]


	with open(stopword_file, "r") as fp:
		lines = fp.readlines()

	stopword_list = set()

	for line in lines:
		stopword_list.add(line.strip())

	exclude_set = {'(', '[', ',', '.', '!', '?', ':', ';', ']', ')'}

	sc = SparkContext(appName="DSCI553Task1", master="local[*]")
	
	scf = SparkConf().setAppName("DSCI553").setMaster("local[*]")
	
	sc = SparkContext.getOrCreate(conf=scf)

	result = dict()

	json_input_content = sc.textFile(input_file).map(lambda row:json.loads(row))

	review_ids = json_input_content.map(lambda kv:kv['review_id'])
	result['A'] = total_num_reviews(review_ids)

	review_years = json_input_content.map(lambda kv:kv['date'])
	result['B'] = number_of_reviews_y(review_years, year)

	business_ids_rdd = json_input_content.map(lambda kv: kv['business_id'])
	result['C'] = number_of_distinct_business(business_ids_rdd)

	business_user_rdd = json_input_content.map(lambda kv:(kv['business_id'], kv['user_id']))
	result['D'] = top_m_users(business_user_rdd, int(m_users))

	business_text_rdd = json_input_content.map(lambda kv: (kv['review_id'], kv['text']))
	result['E'] = most_repeated_words(business_text_rdd, int(n_freq))

	with open(output_file, 'w+') as ofp:
		json.dump(result, ofp, sort_keys=True)

