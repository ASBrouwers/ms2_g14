import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ImportFiles {

    public static JavaRDD removeHeader(JavaRDD inputRDD) {
        String header = (String) inputRDD.first();
        inputRDD = inputRDD.filter(row -> !row.equals(header));
        return inputRDD;
    }

    public static void q1() {
        String startingPath = "/tmp/tables/"; // Folder where table data is located
        String master = "local[1]"; // Run locally with 1 thread

        // Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName(ImportFiles.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
        JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
        JavaPairRDD<Integer, String[]> courseOffersPKandTuple = textCourseOffers.mapToPair(
                row -> new Tuple2<>( Integer.valueOf(row.split(",")[0]), row.split(","))
        );
        JavaPairRDD<Integer, String[]> courseRegistrationsPKandTuple = textCourseRegistrations.mapToPair(
                row -> new Tuple2<>( Integer.valueOf(row.split(",")[0]), row.split(",") )
        );

        JavaPairRDD<Integer, Tuple2<String[], String[]>> join = courseOffersPKandTuple.join(courseRegistrationsPKandTuple);

        // Print first row of join result
        System.out.print(join.first()._1);
        for (String s : join.first()._2._1()) {
            System.out.print(", " + s);
        }
        for (String s : join.first()._2._2()) {
            System.out.print(", " + s + "\n");
        }

    }

    public static void main(String[] args) {
        q1();
    }
}
