package com.achala.apps.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class PracticeFilterStringMaching {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String header = "Segment,Country, Product , Discount Band ,Units Sold, Manufacturing Price , Sale Price , Gross Sales , Discounts ,  Sales , COGS , Profit ,Date,Month Number, Month Name ,Year";

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StringMaching");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> financeRDD = context.textFile("Financial_Sample.csv");
		JavaRDD<String> headerRDD = financeRDD.filter(lines -> lines.equalsIgnoreCase(header));
		System.out.println(headerRDD.count());
		context.close();

	}

}
