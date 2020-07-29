package com.achala.apps.sparkcore;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ClassPractice2 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ClassPractice2").setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> fileRDD = sparkContext.textFile("C:/Users/Piyush Chandra/Desktop/test.txt");
		JavaRDD<String> upperNames = fileRDD.map(f -> f.toUpperCase());
		List<String> names = upperNames.collect();
		System.out.println(names);
		sparkContext.close();

	}

}
