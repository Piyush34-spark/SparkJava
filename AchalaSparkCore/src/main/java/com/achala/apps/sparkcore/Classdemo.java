package com.achala.apps.sparkcore;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Classdemo {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Classdemo");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> textFileRdd = sparkContext.textFile("C:/Users/Piyush Chandra/Desktop/test.txt");
		JavaRDD<String> mapuppernameRdd = textFileRdd.map((xyz) -> xyz.toUpperCase());
		List<String> names = mapuppernameRdd.collect();
		System.out.println(names);
		sparkContext.close();

	}

}
