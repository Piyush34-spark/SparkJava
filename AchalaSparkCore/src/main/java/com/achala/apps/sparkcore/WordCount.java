package com.achala.apps.sparkcore;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("App1");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sparkContext.textFile("C:/Users/Piyush Chandra/Customer.txt");
		JavaRDD<String> words = textFile.flatMap(f -> Arrays.asList(f.split("\\W+")).iterator());
		JavaPairRDD<String, Integer> wordCount = words.mapToPair(f -> new Tuple2<>(f, 1)).reduceByKey((a, b) -> a + b);
//		List<Tuple2<String, Integer>> list = wordCount.collect();
		wordCount.saveAsTextFile("C:/Users/Piyush Chandra/Count.txt");
//		System.out.println(list + "\n");
		sparkContext.close();

	}

}
