import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.ArrayList;
import org.apache.commons.math3.random.MersenneTwister;
import scala.Tuple2;

public class q3 {
	private static int sqrtNrReducers = 6; // nr of reducers: square of this value
	
	public static JavaRDD<String> removeHeader(JavaRDD<String> inputRDD) {
        String header = (String) inputRDD.first();
        inputRDD = inputRDD.filter(row -> !row.equals(header));
        return inputRDD;
    }
	
	public static JavaPairRDD<Integer, Integer> q3b() {
		String startingPath = "/home/student/Desktop/tables/"; // Folder where table data is located
        String master = "local[8]"; // Run locally with 1 thread

        // Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName(q1.class.getName())
                .setMaster(master);
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> studentRegsText = sc.textFile(startingPath + "CourseRegistrations2.table");
        JavaRDD<String> quantilePointsText = sc.textFile(startingPath + "QuantilePoints.table.txt");
        
        // (Grade, G)
        final String gradeHeader = studentRegsText.first();
        JavaPairRDD<Integer, String> studentRegs = studentRegsText.filter(row -> !row.contains("null") && !row.equals(gradeHeader)).mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(Integer.valueOf(split[2]), "G");
        });
        
        // (Quantile, Q)
        final String quantileHeader = quantilePointsText.first();
        JavaPairRDD<Integer, String> quantilePoints = quantilePointsText.filter(row -> !row.equals(quantileHeader)).mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(Integer.valueOf(split[0]), "Q");
        });
		
        // (Grade, G) and (Quantile, Q)
        JavaPairRDD<Integer, String> union = studentRegs.union(quantilePoints);
        
        // Flatmap to get reducer
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> withKeys = union.flatMapToPair(row -> {
        	int[] reducers = q3a(row, sqrtNrReducers);
        	ArrayList<Tuple2<Integer,ArrayList<Tuple2<Integer, String>>>> l = new ArrayList<>();
        	
        	for (int r : reducers) {
        		ArrayList<Tuple2<Integer, String>> rowList = new ArrayList<>();
        		rowList.add(row);
        		l.add(new Tuple2<>(r, rowList));
        	}
        	return l.iterator();
        });
        
        // ArrayList combine all rows at this reducer
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> reduced = withKeys.reduceByKey((V1, V2) -> {
        	ArrayList<Tuple2<Integer, String>> list = V1;
        	list.addAll(V2);
        	return list;
        });
        
        // Split result into 2 ArrayLists
        JavaPairRDD<Integer, Integer> lessThanQuantile = reduced.flatMapToPair(row -> {
        	ArrayList<Tuple2<Integer, String>> allTuples = row._2;
        	
        	ArrayList<Tuple2<Integer, String>> gTuples = new ArrayList<>();
        	ArrayList<Tuple2<Integer, String>> qTuples = new ArrayList<>();
        	
        	for (Tuple2<Integer, String> t : allTuples) {
        		if (t._2.equals("G")) {
        			gTuples.add(t);
        		} else {
        			qTuples.add(t);
        		}
        	}
        	
        	ArrayList<Tuple2<Integer, Integer>> l = new ArrayList<>();
        	
        	for (Tuple2<Integer, String> quantile : qTuples) {
        		for (Tuple2<Integer, String> grade : gTuples) {
        			if (grade._1 <= quantile._1) {
        				l.add(new Tuple2<>(quantile._1, 1));
        			}
        		}
        	}
        	return l.iterator();
        });
        
        JavaPairRDD<Integer, Integer> result = lessThanQuantile.reduceByKey((v1, v2) -> v1 + v2).sortByKey();
        result.collect().forEach(row -> System.out.println(row));
        sc.close();
        return result;
	}
	
	private static int[] q3a(Tuple2<Integer, String> row, int sqrtReducers) {
		MersenneTwister rd = new MersenneTwister();
		int x = rd.nextInt(sqrtReducers);
		int[] reducers = new int[sqrtReducers];
		if (row._2.contentEquals("G")) { // Grade row (Row)
			for (int i = 0; i < sqrtReducers; i++) {
				reducers[i] = x * sqrtReducers + i;
			}
		} else { // Quantile row (Column)
			for (int i = 0; i < sqrtReducers; i++) {
				reducers[i] = sqrtReducers * i + x;
			}
		}
		return reducers;
	}
	
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		q3b();
	}
}
