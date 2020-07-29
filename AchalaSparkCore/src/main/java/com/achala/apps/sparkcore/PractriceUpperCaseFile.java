package com.achala.apps.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PractriceUpperCaseFile {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("UpperFile");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		JavaRDD<String> financialRDD = sparkContext.textFile("Financial_Sample.csv");
		JavaRDD<String> upperfinRDD = financialRDD.map(line -> line.toUpperCase());
		upperfinRDD.take(10).forEach(data -> System.out.println(data));

		sparkContext.close();
	}

}
