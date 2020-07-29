package com.achala.apps.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.*;

public class FinanceAnalysis {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		JavaSparkContext context = new JavaSparkContext(
				new SparkConf().setMaster("local[*]").setAppName("Fanance Anlysis"));

		JavaRDD<String> financeRDD = context.textFile("Financial_Sample.csv");

		JavaRDD<String> prodCarreRDD = financeRDD
				.filter(line -> line.split(",")[2].trim().equalsIgnoreCase("Carretera"));

		JavaRDD<Double> unitSold = prodCarreRDD.map(line -> Double.parseDouble(line.split(",")[4]));

		Double total = unitSold.reduce((a, b) -> a + b);

		Double avg = (double) (total / unitSold.count());

		System.out.println("Total is " + total);
		System.out.println("Average is " + avg);

		context.close();

	}

}
