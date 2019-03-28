import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.MersenneTwister;

import scala.Tuple2;
import scala.Tuple3;

public class q3 {
	
	public static JavaRDD removeHeader(JavaRDD inputRDD) {
        String header = (String) inputRDD.first();
        inputRDD = inputRDD.filter(row -> !row.equals(header));
        return inputRDD;
    }
	
	public static void q3a() {
		MersenneTwister rd = new MersenneTwister();
		
		String startingPath = "/tmp/tables/"; // Folder where table data is located
        String master = "local[1]"; // Run locally with 1 thread

        // Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName(q1.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> studentRegsText = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
        JavaRDD<String> quantilePointsText = removeHeader(sc.textFile(startingPath + "QuantilePoints.table"));
        
        JavaPairRDD<Integer, String> studentRegs = studentRegsText.mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(split[2],"G");
        }).filter(row -> (!row._1.equals("null"))).mapToPair(row -> new Tuple2<>(Integer.valueOf(row._1), row._2));
        
        JavaPairRDD<Integer, String> quantilePoints = quantilePointsText.mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(split[0], "Q");
        }).filter(row -> (!row._1.equals("null"))).mapToPair(row -> new Tuple2<>(Integer.valueOf(row._1), row._2));
		
        // (Grade, G) and (Quantile, Q)
        JavaPairRDD<Integer, String> union = studentRegs.union(quantilePoints);
        
        // ArrayList of 1 row (Integer, String)
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> listed = union.mapToPair(row -> {
        	ArrayList<Tuple2<Integer, String>> l = new ArrayList<>();
        	l.add(row);
        	return new Tuple2<>(row._1, l);
        });
        
        // Flatmap to get reducer
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> withKeys = listed.flatMapToPair(row -> {
        	int x = rd.nextInt(2);
        	int[] reducers;
        	ArrayList<Tuple2<Integer, ArrayList<Tuple2<Integer, String>>>> l = new ArrayList<>();
        	if (row._2.iterator().next()._2.equals("G")) {
        		reducers = getRowKeys(x);
        	} else {
        		reducers = getColKeys(x);
        	}
        	for (int r : reducers) {
        		l.add(new Tuple2<>(r, row._2));
        	}
        	return l.iterator();
        });
        
        //==============> Sent to reducer
        
        // ArrayList combine all rows at this reducer
        JavaPairRDD<Integer, ArrayList<Tuple2<Integer, String>>> reduced = withKeys.reduceByKey((V1, V2) -> {
        	ArrayList list = V1;
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
        
        JavaPairRDD<Integer, Integer> result = lessThanQuantile.reduceByKey((v1, v2) -> v1 + v2);
        result.collect().forEach(row -> System.out.println(row._1 + ": " + row._2));
	}
	
	private static int[] getRowKeys(int row) {
		if (row == 0) {
			return new int[] {1,2};
		} else {
			return new int[] {3,4};
		}
	}
	
	private static int[] getColKeys(int col) {
		if (col == 0) {
			return new int[] {1,3};
		} else {
			return new int[] {2,4};
		}
	}
	
	public static void main(String[] args) {
		q3a();
	}
}
