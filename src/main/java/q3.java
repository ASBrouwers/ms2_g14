import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
	
	public JavaPairRDD<Integer, String[]> q3a(JavaPairRDD<Integer, String[]> rdd1, JavaPairRDD<Integer, String[]> rdd2) {
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
        	return new Tuple2<>(Integer.valueOf(split[2]),"G");
        });
        
        JavaPairRDD<Integer, String> quantilePoints = quantilePointsText.mapToPair(row -> {
        	String[] split = row.split(",");
        	return new Tuple2<>(Integer.valueOf(split[0]), "Q");
        });
		
        JavaPairRDD<Integer, String> union = studentRegs.union(quantilePoints);
        
        JavaPairRDD<Integer, ArrayList<Tuple3<String, Integer, Integer>>> result = union.mapToPair(row -> {
        	if (row._2 == "G") {
        		int r = rd.nextInt(2);
        		for (int region : getRowKeys(r)) {
        			ArrayList<Tuple3<String, Integer, Integer>> list = new ArrayList<>();
        			list.add(new Tuple3<>("G", 1, row._1));
        			return new Tuple2<>(region, list);
        		}
        	} else { // "Q"
        		int c = rd.nextInt(2);
        		for (int region : getColKeys(c)) {
        			ArrayList<Tuple3<String, Integer, Integer>> list = new ArrayList<>();
        			list.add(new Tuple3<>("Q", 1, row._1));
        			return new Tuple2<>(region, list);
        		}
        	}
        })
        		.reduceByKey((V1, V2) -> {
        			ArrayList list = V1;
        			list.addAll(V2);
        			return list;
        		});
        
        String str = "";
        List<Tuple2<Integer, ArrayList<Tuple3<String, Integer, Integer>>>> test = result.take(10);
        for (Tuple2<Integer, ArrayList<Tuple3<String, Integer, Integer>>> t : test) {
        	str += t._1 + "\n";
        	for (Tuple3<String, Integer, Integer> s : t._2) {
        		str += s._1() + ", " + s._3();
        	}
        }
        System.out.println(str);
        
		
		return new JavaPairRDD<Integer, String[]>(null, null, null);
	}
	
	private int[] getRowKeys(int row) {
		if (row == 0) {
			return new int[] {1,2};
		} else {
			return new int[] {3,4};
		}
	}
	
	private int[] getColKeys(int col) {
		if (col == 0) {
			return new int[] {1,3};
		} else {
			return new int[] {2,4};
		}
	}
}
