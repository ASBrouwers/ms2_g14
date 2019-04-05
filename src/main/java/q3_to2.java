import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.ArrayList;

import scala.Tuple2;

public class q3_to2 {
	public static JavaRDD<String> removeHeader(JavaRDD<String> inputRDD) {
        String header = (String) inputRDD.first();
        inputRDD = inputRDD.filter(row -> !row.equals(header));
        return inputRDD;
    }
	
	public static void q3b(int courseId, int year, int quartile) {
		String startingPath = "/home/student/Desktop/tables/"; // Folder where table data is located
        String master = "local[8]"; // Run locally with 1 thread

        // Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName(q1.class.getName())
                .setMaster(master);
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> studentRegsText = sc.textFile(startingPath + "CourseRegistrations.table");
        String studentRegsHeader = studentRegsText.first();
        studentRegsText = studentRegsText.filter(row -> !row.equals(studentRegsHeader) && !row.contains("null"));
        JavaRDD<String> courseOffersText = sc.textFile(startingPath + "CourseOffers.table");
        String coHeader = courseOffersText.first();
        courseOffersText = courseOffersText.filter(row -> !row.equals(coHeader));
        
        // (OfferId, (Grade, G))
        JavaPairRDD<Integer,Tuple2<Integer, String>> studentRegs = studentRegsText
        	.mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(Integer.valueOf(split[0]), new Tuple2<>(Integer.valueOf(split[2]) ,"G"));
        });

        
        JavaPairRDD<Integer, Tuple2<Integer, String>> studentRegsFiltered = studentRegs
        		.filter(row -> row._2._1 >= 5);
        
        // Setup and filter courseOffers
        JavaPairRDD<Integer, Tuple2<Integer[], String>> courseOffers = courseOffersText
        	.mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(Integer.valueOf(split[0]), new Tuple2<> (new Integer[]{Integer.valueOf(split[1]), Integer.valueOf(split[2]), Integer.valueOf(split[3])}, "O"));
        });
        
		
        // (CourseOfferId, (courseId, O))
        JavaPairRDD<Integer, Tuple2<Integer, String>> courseOffersFiltered = courseOffers
        		.filter(row -> row._2._1[0] == courseId && row._2._1[1] == year && row._2._1[2] == quartile )
        		.mapToPair(row -> new Tuple2<>(row._1, new Tuple2<>(row._2._1[0] ,row._2._2)));
        
        // union
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> union = studentRegsFiltered.union(courseOffersFiltered).mapToPair(row -> {
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
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> sumCount = reduced.flatMapToPair(row -> {
        	ArrayList<Tuple2<Integer, String>> allTuples = row._2;
        	
        	ArrayList<Tuple2<Integer, String>> gTuples = new ArrayList<>();
        	ArrayList<Tuple2<Integer, String>> oTuples = new ArrayList<>();
        	
        	for (Tuple2<Integer, String> t : allTuples) {
        		if (t._2.equals("G")) {
        			gTuples.add(t);
        		} else {
        			oTuples.add(t);
        		}
        	}
        	
        	ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> l = new ArrayList<>();
        	
        	for (Tuple2<Integer, String> offer : oTuples) {
	        	for (Tuple2<Integer, String> courseReg : gTuples) {
	        		l.add(new Tuple2<>(offer._1, new Tuple2<>(1, courseReg._1)));
	        	}
        	}
        	return l.iterator();
        });
        
        // Reduce on CourseId
        sumCount = sumCount.reduceByKey((v1, v2) -> {
        	int count = v1._1 + v2._1;
        	int sum = v1._2 + v2._2;
        	return new Tuple2<>(count, sum);
        });
        
        JavaPairRDD<Integer, Double> result = sumCount.mapToPair(row -> new Tuple2<>(row._1, (double)row._2._2 / row._2._1));
        result.collect().forEach(row -> System.out.println(row));
        sc.close();
	}
	
	
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		q3b(25079,2010,3);
	}
}
