package org.inet.test;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class test {
	
	public static void main(String[] args) {
	
	JavaSparkContext context = new JavaSparkContext(
			new SparkConf().setMaster("spark://192.168.174.1:7077").setAppName("Finance Analysis product wise"));

	JavaRDD<String> financeRDD = context.textFile("Financial_Sample.csv");
	/*
	 * JavaPairRDD<String, Integer> pairRDD = financeRDD.mapToPair( data -> new
	 * Tuple2<String, Integer>(data.split(",")[2].trim(),
	 * Integer.parseInt((data.split(",")[4]))));
	 */

	JavaPairRDD<String, Double> pairRDD = financeRDD
			.mapToPair(data -> new Tuple2<String, Double>(data.split(",")[2].trim(),
					Double.parseDouble(data.split(",")[4])));
	JavaPairRDD<String, Double> prodTotSales = pairRDD.reduceByKey((a, b) -> a + b);
	
	List<Tuple2<String, Double>> sales = prodTotSales.collect();
	
	sales.forEach(line -> System.out.println("Product = " + line._1 +" has "+ "Total Sales = " + line._2));

	
	context.close();
	
}


}
