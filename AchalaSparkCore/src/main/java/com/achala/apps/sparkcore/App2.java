package com.achala.apps.sparkcore;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.log4j.*;

import scala.Tuple2;

public class App2 {

	public static void main(String... args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("App2");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sparkContext.textFile("C:/Users/Piyush Chandra/Customer.txt");
		textFile.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\\W+")).iterator();
			}

		});
		JavaRDD<String> words = textFile.flatMap(f -> Arrays.asList(f.split("\\W+")).iterator());
		JavaPairRDD<String, Integer> wordCount = words.mapToPair(f -> new Tuple2<>(f, 1)).reduceByKey((a, b) -> a + b);

		List<Tuple2<String, Integer>> result = wordCount.collect();
		result.forEach(x -> System.out.println(x));
		sparkContext.close();
	}

}
