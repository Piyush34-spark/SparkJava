package com.achala.apps.sparkcore;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class EmailMaching {

	public static void main(String[] args) {
		String emailRegex = "^[a-zA-Z0-9_+&*-]+(?:\\."+ 
                "[a-zA-Z0-9_+&*-]+)*@" + 
                "(?:[a-zA-Z0-9-]+\\.)+[a-z" + 
                "A-Z]{2,7}$";
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Email Validation");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> emailFile = context.textFile("Emails.txt");
		JavaRDD<String> validEmail = emailFile.filter(email -> Pattern.compile(emailRegex).matcher(email).matches());
		validEmail.foreach(email -> System.out.println(email));
		context.close();
	}

}
