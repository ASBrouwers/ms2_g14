import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/*
 * Implementation of equi-join in Spark
 * find the average grade of all students that passed the course
 * (i.e. grade >= 5) with CourseId = %1% at the %2% quartile of year %3%
 * The result should be printed
 * 
 * Not allowed to use Spark's support for SQL, datasets and dataframes
 * Only use pure Rdds with corresponding Spark methods with Java primitives
 * 
 * SQL equivalent to this code:
 * SELECT AVG(GRADE) FROM StudentRegistrations, CourseOffers
 * 	WHERE StudentRegistrations.CourseOfferId = CourseOffers.CourseOfferId
 * 	 AND StudentRegistrations.grade >= 5
 * 	 AND CourseOffers.CourseId = %1%
 *   AND CourseOffers.Quartile = %2%
 *   AND CourseOffers.Year = %3%
 *   
 * Schema of CourseRegistrations
 * 	CourseOfferId FK, int
 *  StudentRegistrationId FK, int
 *  Grade int
 * 
 * Schema of CourseOffers
 * 	CourseOfferId int
 *  CourseId FK, int
 *  Year int
 *  Quartile int
 *  
 *  Approach
 *  	1. filter tables using %1%, %2%, and %3% (reduce size of tables 
 *  		to be joined as much as possible before the join takes place
 *  	2. execute equi-join
 *  
 *  Equi-join pseudo code
 *  	1. read files into RDDs - done
 * 		2. convert each RDD into key,value - done
 * 		3. union the pair RDDs
 * 			* are we allowed to use: 
 * 				union(JavaPairRDD<K,V> other) 
 *				Return the union of this RDD and another one? - yes
 * 		4. reduce on the key
 * 			* use a filter: keys match? - use reducebykey - ask on friday
 * */

public class q2 {
	static int courseId = 1127;
	static int quartile = 3;
	static int year = 2007;
	
	public static JavaRDD removeHeader(JavaRDD inputRDD) {
        String header = (String) inputRDD.first();
        inputRDD = inputRDD.filter(row -> !row.equals(header));
        return inputRDD;
    }
	
	public static JavaRDD<String> addIdentifier(JavaRDD input, String id) {
		JavaRDD<String> output = input.map(row -> row += "," + id);
		return output;
	}
	
	public static void printFirstLinePairRDD(String desc, JavaPairRDD<Integer, String[]> input) {
		String output = "\n" + desc + ", first line\n";
		for (int i = 0; i < input.first()._2.length; i++ ) {
        	output += input.first()._2[i] + ", ";
        }
        output += "\n\n";
        System.out.println(output);
	}
	
	public static void printEntirePairRDD(String desc, JavaPairRDD<Integer, String[]> input) {
		System.out.println(desc);
		input.foreach(row -> {
			String output = "key: " + row._1() + ", values: ";
			for (int i = 0; i < row._2().length; i++) {
				output += row._2()[i] + ", ";
			}
			System.out.println(output);
		});
	}
	
	public static void query2() {
		String startingPath; // Folder where table data is located
		//startingPath = "/tmp/tables/"; 
		startingPath = "/tmp/tables_reduced/";
        String master = "local[1]"; // Run locally with 1 thread

        // Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName(q2.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        // unneeded for final project
        JavaRDD<String> textCourseOffersRaw = sc.textFile(startingPath + "CourseOffers.table");
        System.out.print("\n" + textCourseOffersRaw.first() + "\n\n");
        JavaRDD<String> textCourseRegistrationsRaw = sc.textFile(startingPath + "CourseRegistrations.table");
        System.out.print("\n" + textCourseRegistrationsRaw.first() + "\n\n");
        // end unneeded for final project
        
        JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
        JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
        JavaRDD<String> textCourseOffersId = addIdentifier(textCourseOffers,"CO");
        JavaRDD<String> textCourseRegistrationsId = addIdentifier(textCourseRegistrations,"CR");
        
        JavaPairRDD<Integer, String[]> courseOffersPKandTuple = textCourseOffersId.mapToPair(
                row -> new Tuple2<>( Integer.valueOf(row.split(",")[0]), row.split(",")) // first column is the PK
        );
        printFirstLinePairRDD("CourseOffers", courseOffersPKandTuple);
        
        JavaPairRDD<Integer, String[]> courseRegistrationsPKandTuple = textCourseRegistrationsId.mapToPair(
                row -> new Tuple2<>( Integer.valueOf(row.split(",")[0]), row.split(","))// first column is the PK
        );
        printFirstLinePairRDD("CourseRegistrations",courseRegistrationsPKandTuple);
        
        // filter courseOffers
        printEntirePairRDD("CourseOffers, unfiltered", courseOffersPKandTuple);
        // keep CourseId = var, CourseId is stored in the 2nd column
        JavaPairRDD<Integer, String[]> courseOffersFilterCourseId = courseOffersPKandTuple.filter(tuple -> Integer.parseInt(tuple._2()[1]) == courseId);
        printEntirePairRDD("CourseOffers, filtered on CourseId", courseOffersFilterCourseId);
        // keep Quartile = var, Quartile is stored in the 4th column
        JavaPairRDD<Integer, String[]> courseOffersFilterCourseIdQuartile = courseOffersFilterCourseId.filter(tuple -> Integer.parseInt(tuple._2()[3]) == quartile);
        printEntirePairRDD("CourseOffers, filtered on CourseId and Quartile", courseOffersFilterCourseIdQuartile);
        // keep Year = var, Year is stored in the 3rd column
        JavaPairRDD<Integer, String[]> fullFilterCourseOffers = courseOffersFilterCourseIdQuartile.filter(tuple -> Integer.parseInt(tuple._2()[2]) == year);
        printEntirePairRDD("CourseOffers, fully filtered; CourseId, Quartile, and Year", fullFilterCourseOffers);
        
        // filter courseRegistrations - done
        printEntirePairRDD("CourseRegistrations, unfiltered",courseRegistrationsPKandTuple);
        // remove grade = "null"
        JavaPairRDD<Integer, String[]> courseRegistrationsPKandTupleNoNull = courseRegistrationsPKandTuple.filter(row -> !row._2()[2].contentEquals("null"));
        printEntirePairRDD("CourseRegistrations, removed null",courseRegistrationsPKandTupleNoNull);
        // keep grade >= 5
        JavaPairRDD<Integer, String[]> fullFilterCourseRegistrations = courseRegistrationsPKandTupleNoNull.filter(row -> Integer.parseInt(row._2()[2]) >= 5);
        printEntirePairRDD("CourseRegistrations, filtered on grade >= 5", fullFilterCourseRegistrations);
        
        // union courseOffers and courseRegistrations - not done
        JavaPairRDD<Integer, String[]> CourseOffersAndCourseRegistrationsUnion = fullFilterCourseOffers.union(fullFilterCourseRegistrations);
        printEntirePairRDD("CourseOffers and CourseRegistrations union result", CourseOffersAndCourseRegistrationsUnion);
        
        // reduce by key on union'd RDD
        //JavaPairRDD<Integer, String[]> finalRDD = ((Object) CourseOffersAndCourseRegistrationsUnion).reducebyKey((x,integer y) -> x+y);
        

    	//(Function2<Long, Long, Long>) (a, b) -> a + b
        
        // calculate average grade
        
        // shutdown the spark context
        sc.close();
	}
	
	public static Integer[] createIntegerArray(String[] input) {
		Integer[] result = new Integer[input.length];
		for (int i = 0; i < result.length; i++) {
			if (!input[i].contentEquals("null")) {
				result[i] = Integer.parseInt(input[i]);
			} else {
				result[i] = -1;
			}
		}
		return result;
	}
	
	public static void printPartPairRDD(JavaPairRDD<Integer, Integer[]> input, int limit, String description) {
		int actual_size = (int) input.count();
		System.out.println(description + "\n RDD has size " + actual_size + "\n");
		if (actual_size < limit) {
			limit = actual_size;
			System.out.println("PairRDD length is smaller than limit parameter");
			input.foreach(row -> {
				String output = "key: " + row._1() + ", values: ";
				for (int i = 0; i < row._2().length; i++) {
					output += row._2()[i] + ", ";
				}
				System.out.println(output);
			});
		} else {
			input.take(limit).forEach(row -> {
				String output = "key: " + row._1() + ", values: ";
				for (int i = 0; i < row._2().length; i++) {
					output += row._2()[i] + ", ";
				}
				System.out.println(output);
			});
		}
		
	}
	

	public static void query2alternative() {
		String startingPath; // Folder where table data is located
		//startingPath = "/tmp/tables/"; 
		startingPath = "/tmp/tables_reduced/";
        String master = "local[1]"; // Run locally with 1 thread

        // Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName(q2.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
        JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
        JavaRDD<String> textCourseOffersId = addIdentifier(textCourseOffers,"0");
        JavaRDD<String> textCourseRegistrationsId = addIdentifier(textCourseRegistrations,"1");
        
        JavaPairRDD<Integer, Integer[]> courseOffers = textCourseOffersId.mapToPair(
                row -> new Tuple2<>( Integer.valueOf(row.split(",")[0]), createIntegerArray(row.split(","))) // first column is the PK
        );
        
        JavaPairRDD<Integer, Integer[]> courseRegistrations = textCourseRegistrationsId.mapToPair(
                row -> new Tuple2<>( Integer.valueOf(row.split(",")[0]), createIntegerArray(row.split(",")))// first column is the PK
        );
        
        // filter courseOffers - done    
        // keep CourseId = var, CourseId is stored in the 2nd column
        // keep Quartile = var, Quartile is stored in the 4th column
        // keep Year = var, Year is stored in the 3rd column
        printPartPairRDD(courseOffers, 40, "CourseOffers, prior to filtering");
        printPartPairRDD(courseOffers, 2, "CourseOffers, prior to filtering - part");
        courseOffers = courseOffers.filter(tuple -> tuple._2()[1] == courseId);        
        courseOffers = courseOffers.filter(tuple -> tuple._2()[3] == quartile);
        courseOffers = courseOffers.filter(tuple -> tuple._2()[2] == year);
        printPartPairRDD(courseOffers, 40, "CourseOffers, after filtering");
        
        
        // filter courseRegistrations - done
        // remove grade = "null" - already done in RDD creation process       
        // keep grade >= 5
        printPartPairRDD(courseRegistrations,50,"CourseRegistrations, prior to filtering");
        courseRegistrations = courseRegistrations.filter(tuple -> tuple._2()[2] >= 5);
        printPartPairRDD(courseRegistrations,50,"CourseRegistrations, after filtering");        
        
        
        // union courseOffers and courseRegistrations - not done
        JavaPairRDD<Integer, Integer[]> union = courseOffers.union(courseRegistrations);
        printPartPairRDD(union, 50, "union");
        
        // filter to only keep matching pairs
        
        // get sum and count
        
        // calculate average grade
        
        // shutdown the spark context
        sc.close();
	}
	
	public static void main(String[] args) {
		//query2();
		query2alternative();
	}

}
