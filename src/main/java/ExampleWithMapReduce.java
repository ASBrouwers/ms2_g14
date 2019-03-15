import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;
import scala.Tuple2;

/**
 * Example class
 */
public class ExampleWithMapReduce {
    /**
     * We use a logger to print the output. Sl4j is a common library which works with log4j, the
     * logging system used by Apache Spark.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleWithMapReduce.class);

    /**
     * This is the entry point function when the task is called with spark-submit.sh from command
     * line. In our example we will call the task from a WordCountTest instead.
     * See {@see http://spark.apache.org/docs/latest/submitting-applications.html}
     */
    public static JavaRDD removeHeader(JavaRDD textRDD) {
        String header = (String) textRDD.first();
        textRDD = textRDD.filter(row -> !row.equals(header));
        return textRDD;
    }

    public static void main(String[]args) {
        equijoins();
//    	thetajoins();
    }

    @SuppressWarnings("serial")
    public static void equijoins() {
    	String startingpath = "/home/student/";

        String master = "local[1]";

        /*
         * Initializes a Spark context.
         */
        SparkConf conf = new SparkConf()
                .setAppName(ExampleWithMapReduce.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read the two text files
        JavaRDD<String> textRDDCourses = removeHeader(sc.textFile(startingpath + "Courses.table"));
        JavaRDD<String> textRDDCourseOffers = removeHeader(sc.textFile(startingpath + "CourseOffers.table"));


        // Pair RDD should be as: int PK, Tuple2<tableid, String[]>
        // Pair function: Input string, produces <int PK, Tuple2 <tableType, String[] data>>
        JavaPairRDD< Integer, String[] > CoursesToPKAndRecordTuple = textRDDCourses.mapToPair(new PairFunction<String, Integer, String[] > () {
            @Override
            public Tuple2<Integer,String[]> call(String record) throws Exception {
                String[] result = record.split(",");
                Tuple2<Integer,String[]> t = new Tuple2(new Integer(result[0]), result);
                return t;
            }
        });

        JavaPairRDD<Integer, String[]> CourseOffersToPKAndRecordTuple = textRDDCourseOffers.mapToPair((PairFunction<String, Integer, String[]>) record -> {
            String[] result = record.split(",");
            Tuple2<Integer, String[]> t = new Tuple2(new Integer(result[1]), result);
            return t;
        });


       
        // Join by using join
        {
            JavaPairRDD<Integer, Tuple2<String[], String[]>> joinWithJoin = CoursesToPKAndRecordTuple.join(CourseOffersToPKAndRecordTuple);
            System.out.println("Join using join - Size of results is " + joinWithJoin.count());
        }
        
        // join by using MR -- this is part of your milestone
        {
        	
        }

    }

    /**
     * The task body
     */
    public void run(String inputFilePath) {

    }
}
