package com.achala.apps.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PracticeFilterIgnorHeader {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("IgnorHeader");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> financeRDD = context.textFile("Financial_Sample.csv");
		String header = financeRDD.first();
		JavaRDD<String> cleanFinRDD = financeRDD.filter(lines -> !lines.equalsIgnoreCase(header));
		System.out.println(cleanFinRDD.count());
		context.close();

	}

}
