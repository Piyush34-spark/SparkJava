package com.achala.apps.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PracticeFilter {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PracticeFilter");
		JavaSparkContext SparkContext = new JavaSparkContext(conf);
		JavaRDD<String> FinanceRDD = SparkContext.textFile("Financial_Sample.csv");
		JavaRDD<String> governmentRDD = FinanceRDD.filter(line -> line.contains("Government"));
		System.out.println(governmentRDD.count());
		SparkContext.close();

	}

}
