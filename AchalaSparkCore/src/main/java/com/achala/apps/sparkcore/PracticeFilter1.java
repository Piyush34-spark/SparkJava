package com.achala.apps.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PracticeFilter1 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("PracticeFilter1");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> FinanceRDD = context.textFile("Financial_Sample.csv");
		JavaRDD<String> vttFilterRDD = FinanceRDD.filter(data -> data.split(",")[2].trim().equals("VTT"));
		System.out.println(vttFilterRDD.count());
		context.close();
	}

}
