import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class q2extra {	
	public static void testSumReducer() {
		String function_result = "";
		Integer[] raw_grades = {7,6,8,9,6,8,5,6,9};
		// no RDD calculation
		float ref_sum = 0;
		float ref_count = raw_grades.length;
		for (Integer i : raw_grades) {
			ref_sum += i;
		}
		float ref_avg = ref_sum / ref_count;
		function_result += "reference average = " + ref_avg + ", reference sum = " + ref_sum + ", reference count = " + ref_count + ")\n";
		
		// RDD calculation
		String master = "local[2]"; // Run locally with 1 thread
		SparkConf conf = new SparkConf().setAppName(q2old.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> data = Arrays.asList(raw_grades);
		JavaRDD<Integer> gradeRDD = sc.parallelize(data);
		JavaPairRDD<Integer, Integer> grades = gradeRDD.mapToPair(tuple -> {
			Integer k = 1;
			Integer v = tuple;
			return new Tuple2<>(k,v);
		});
		grades.foreach(tuple -> System.out.println(tuple));
		
		JavaPairRDD<Integer, Integer[]> pre_reduce = grades.mapToPair(tuple -> {
			Integer k = tuple._1();
			Integer[] v = new Integer[2];
			v[0] = tuple._2(); // will become sum
			v[1] = 1; // will become count
			return new Tuple2<>(k,v);
		});
		pre_reduce.foreach(tuple -> {
			String out = "(" + tuple._1() + "," + tuple._2()[0] + "," + tuple._2()[1] + ")";
			System.out.println(out);
		});
		
		JavaPairRDD<Integer, Integer[]> sumAndCount = pre_reduce.reduceByKey((a,b) -> {
			Integer[] out = new Integer[2];
			out[0] = a[0] + b[0];
			out[1] = a[1] + b[1];
			return out;
		});
		sumAndCount.foreach(tuple -> {
			String out = "(" + tuple._1() + "," + tuple._2()[0] + "," + tuple._2()[1] + ")";
			System.out.println(out);
		});
		
		float rdd_avg = (float) sumAndCount.first()._2()[0] / (float) sumAndCount.first()._2()[1];
		function_result += "RDD computed average = " + rdd_avg + ", RDD sum = " + (float) sumAndCount.first()._2()[0] + ", RDD count = " + (float) sumAndCount.first()._2()[1] + ")\n";
		
		// print results
		sc.close();
		System.out.println(function_result);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		testSumReducer();
	}

}
