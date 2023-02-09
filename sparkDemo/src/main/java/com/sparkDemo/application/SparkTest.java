package com.sparkDemo.application;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

public class SparkTest {
	public static void main(String[] args) {

		SparkSession session = SparkSession.builder().appName("sparkexample").master("local[3]").getOrCreate();

		try (JavaSparkContext context = new JavaSparkContext(session.sparkContext())) {
			List<Integer> integers = Arrays.asList(1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

			JavaRDD<Integer> javaRDD = context.parallelize(integers, 3);

			javaRDD.foreach((VoidFunction<Integer>) integer -> {

				System.out.println("Java RDD:" + integer);
				Thread.sleep(3000);
			});
																			
			Thread.sleep(1000000);
			context.stop();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
