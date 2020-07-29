package com.achala.apps.sparkcore;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.*;

public class ClassPractice1 {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("ClassPractice1").setMaster("local[*]");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> fileRDD = sparkContext.textFile("C:/Users/Piyush Chandra/Desktop/test.txt");
		JavaRDD<String> namesRDD = fileRDD.map(f -> f.substring(f.indexOf(".") + 1, f.indexOf(",")));
		List<String> names = namesRDD.collect();
		System.out.println(names);
		sparkContext.close();

	}

}
