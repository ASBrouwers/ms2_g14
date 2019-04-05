import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.ArrayList;

import scala.Tuple2;


public class q2 {
	public static JavaRDD<String> removeHeader(JavaRDD<String> inputRDD) {
		String header = (String) inputRDD.first();
		inputRDD = inputRDD.filter(row -> !row.equals(header));
		return inputRDD;
	}
	
	public static String query2(int courseId, int quartile, int year) {
		String function_output = "";
		String startingPath = "/tmp/tables/";
		String master = "local[4]"; // Run locally with 4 threads

		// Setup Spark
		SparkConf conf = new SparkConf().setAppName(q2old.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
		JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));

		// first column is the PK
		JavaPairRDD<Integer, String[]> courseOffers = textCourseOffers
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), row.split(",")));
				
		// first column is the PK
		JavaPairRDD<Integer, String[]> courseRegistrations = textCourseRegistrations
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), row.split(",")));
				
		// filter courseOffers
		// keep CourseId = var, CourseId is stored in the 2nd column
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[1]) == courseId);
		// keep Quartile = var, Quartile is stored in the 4th column
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[3]) == quartile);
		// keep Year = var, Year is stored in the 3rd column
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[2]) == year);

		// filter courseRegistrations
		// remove grade = "null"
		courseRegistrations = courseRegistrations.filter(tuple -> !tuple._2()[2].contentEquals("null"));
		// keep grade >= 5
		courseRegistrations = courseRegistrations.filter(tuple -> Integer.parseInt(tuple._2()[2]) >= 5);

		// convert courseOffers for join
		String co_table_id = "CO";
		// tuple layout: K(CourseOfferId), V(CourseId,co_table_id)
		JavaPairRDD<Integer, Tuple2<Integer, String>> courseOffersPreJoin = courseOffers.mapToPair(tuple -> {
			Integer k = tuple._1();
			Tuple2<Integer, String> v = new Tuple2<>(Integer.parseInt(tuple._2()[1]),co_table_id);
			return new Tuple2<>(k,v);
		});
		
		// convert courseRegistrations for join
		String cr_table_id = "CR";
		// tuple layout: K(CourseOfferId), V(Grade, cr_table_id)
		JavaPairRDD<Integer, Tuple2<Integer, String>> courseRegistrationsPreJoin = courseRegistrations.mapToPair(tuple -> {
			Integer k = tuple._1();
			// only store grade and table identifier in new tuple
			Tuple2<Integer,String> v = new Tuple2<>(Integer.parseInt(tuple._2()[2]),cr_table_id);
			return new Tuple2<>(k,v);
		});
		
        // union
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> union = courseRegistrationsPreJoin.union(courseOffersPreJoin).mapToPair(row -> {
        	ArrayList<Tuple2<Integer, String>> list = new ArrayList<>();
        	list.add(row._2);
        	return new Tuple2<>(row._1, list);
        });
        
        // Reduce on courseOfferId
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> reduced = union.reduceByKey((V1, V2) -> {
        	ArrayList<Tuple2<Integer, String>> list = V1;
        	list.addAll(V2);
        	return list;
        });
        
        // Assign courseId keys to grades
        // tuple layout: K(CourseOfferId), V(Tuple2(K: CourseId, V: Grade))
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseIdAndGrades = reduced.flatMapToPair(row -> {
        	ArrayList<Tuple2<Integer, String>> allTuples = row._2;
        	
        	ArrayList<Tuple2<Integer, String>> crTuples = new ArrayList<>();
        	ArrayList<Tuple2<Integer, String>> coTuples = new ArrayList<>();
        	
        	for (Tuple2<Integer, String> t : allTuples) {
        		if (t._2.equals(cr_table_id)) {
        			crTuples.add(t);
        		} else {
        			coTuples.add(t);
        		}
        	}
        	
        	ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> l = new ArrayList<>();
        	
        	for (Tuple2<Integer, String> offer : coTuples) {
	        	for (Tuple2<Integer, String> courseReg : crTuples) {
	        		l.add(new Tuple2<>(offer._1, new Tuple2<>(1, courseReg._1)));
	        	}
        	}
        	return l.iterator();
        });
        
        
        JavaPairRDD<Integer, Integer[]> pre_reduce = courseIdAndGrades.mapToPair(tuple -> {
			Integer k = tuple._1();
			Integer[] v = new Integer[2];
			v[0] = tuple._2._2; // will become sum
			v[1] = 1; // will become count
			return new Tuple2<>(k,v);
		});
		
		JavaPairRDD<Integer, Integer[]> sumAndCount = pre_reduce.reduceByKey((a,b) -> {
			Integer[] out = new Integer[2];
			out[0] = a[0] + b[0];
			out[1] = a[1] + b[1];
			return out;
		});
        
		float average = (float) sumAndCount.first()._2()[0] / (float) sumAndCount.first()._2()[1];;
		
		// close spark
		sc.close();
		
		function_output += "Average grade (Spark): " + average + ", for CourseId = " + courseId + ", Quartile = " + quartile + ", Year = " + year + ", and all grades >= 5";
		return function_output;
	}
	
	public static void performTests() {
		String test_results = "\n\n** Results ** \n";
		test_results += query2(1127,3,2007) + "\n";
		test_results += "Average grade (SQL): 6.3652173913043478, for CourseId = 1127, Quartile = 3, Year = 2007, and all grades >= 5\n";
		
		test_results += query2(23246,4,2017) + "\n";
		test_results += "Average grade (SQL): 5.5972222222222222, for CourseId = 23246, Quartile = 4, Year = 2017, and all grades >= 5\n";
		
		test_results += query2(12457,2,2014) + "\n";
		test_results += "Average grade (SQL): 7.0606060606060606, for CourseId = 12457, Quartile = 2, Year = 2014, and all grades >= 5\n";
		
		test_results += query2(31814,4,2015) + "\n";
		test_results += "Average grade (SQL): 6.4247787610619469, for CourseId = 31814, Quartile = 4, Year = 2015, and all grades >= 5\n";
		System.out.println(test_results);
	}
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		//performTests();
		//perform one run with custom parameters
		int courseId = %1%;
		int quartile = %2%;
		int year = %3%;
		query2(courseId,quartile,year);
	}

}
