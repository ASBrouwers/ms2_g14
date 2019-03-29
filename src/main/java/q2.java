import java.util.*;

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
	
	// [keep] contains an array of the columns that should remain in the output row
	public static Integer[] filterRow(Integer[] input, int[] keep) {
		List<Integer> result_list = new ArrayList<Integer>();
		for (int i = 0 ; i < input.length; i++) {
			for (int k = 0; k < keep.length; k++) {
				if (i == keep[k]) {
					result_list.add(input[i]);
				}
			}
		}
		Integer[] result = new Integer[result_list.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = result_list.get(i);
		}
		return result;
	}
	
	// copy contents from one array to another, assumes both arrays are initialized as empty arrays
	/*public static void copyToArray(Integer[] input, Integer[] output) {
		if (input.length == output.length) {
			for (int i = 0; i < input.length; i++) {
				output[i] = input[i];
			}
		} else {
			System.out.println("error copying array");
		}
	}*/
	
	public static List<Integer> createIntegerList(Integer[] content, int[] cols) {
		List<Integer> output = new ArrayList<Integer>();
		if (cols != null) {
			for (int i = 0; i < content.length; i++) {
				for (int c = 0; c < cols.length; c++) {
					if (i == cols[c]) {
						output.add(content[i]);
					}
				}
			}
		}
		return output;
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

	public static void printPartPairListRDD(JavaPairRDD<Integer, List<Integer>> input, int limit, String description) {
		int actual_size = (int) input.count();
		System.out.println(description + "\n RDD has size " + actual_size + "\n");
		if (actual_size < limit) {
			limit = actual_size;
			System.out.println("PairRDD length is smaller than limit parameter");
			input.foreach(row -> {
				String output = "key: " + row._1() + ", values: ";
				for (int i = 0; i < row._2().size(); i++) {
					output += row._2().get(i) + ", ";
				}
				System.out.println(output);
			});
		} else {
			input.take(limit).forEach(row -> {
				String output = "key: " + row._1() + ", values: ";
				for (int i = 0; i < row._2().size(); i++) {
					output += row._2().get(i) + ", ";
				}
				System.out.println(output);
			});
		}
	}

	
	public static void query2alternative() {
		String startingPath; // Folder where table data is located
		// startingPath = "/tmp/tables/";
		startingPath = "/tmp/tables_reduced/";
		String master = "local[1]"; // Run locally with 1 thread

		// Setup Spark
		SparkConf conf = new SparkConf().setAppName(q2.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
		JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
		JavaRDD<String> textCourseOffersId = addIdentifier(textCourseOffers, "0");
		JavaRDD<String> textCourseRegistrationsId = addIdentifier(textCourseRegistrations, "1");

		// first column is the PK
		JavaPairRDD<Integer, Integer[]> courseOffers = textCourseOffersId
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), createIntegerArray(row.split(","))));

		// first column is the PK
		JavaPairRDD<Integer, Integer[]> courseRegistrations = textCourseRegistrationsId
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), createIntegerArray(row.split(","))));

		// filter courseOffers - done
		// keep CourseId = var, CourseId is stored in the 2nd column
		// keep Quartile = var, Quartile is stored in the 4th column
		// keep Year = var, Year is stored in the 3rd column
		printPartPairRDD(courseOffers, 40, "CourseOffers, prior to filtering");
		printPartPairRDD(courseOffers, 2, "CourseOffers, prior to filtering - part");
		// courseOffers = courseOffers.filter(tuple -> tuple._2()[1] == courseId);
		// courseOffers = courseOffers.filter(tuple -> tuple._2()[3] == quartile);
		// courseOffers = courseOffers.filter(tuple -> tuple._2()[2] == year);
		printPartPairRDD(courseOffers, 40, "CourseOffers, after filtering");

		// filter courseRegistrations - done
		// remove grade = "null" - already done in RDD creation process
		// keep grade >= 5
		printPartPairRDD(courseRegistrations, 50, "CourseRegistrations, prior to filtering");
		// courseRegistrations = courseRegistrations.filter(tuple -> tuple._2()[2] >= 5);
		printPartPairRDD(courseRegistrations, 50, "CourseRegistrations, after filtering");

		// alter tables to use the following layout:
		// courseOffers: key = key, values = none
		int[] keepthesecolsco = {};
		JavaPairRDD<Integer, Integer[]> courseOffersMin = courseOffers.mapToPair(row -> new Tuple2<>(row._1(), filterRow(row._2(),keepthesecolsco)));
		printPartPairRDD(courseOffersMin,100,"CourseOffers, all unneeded columns removed");
		
		// courseRegistrations: key = key, values = grade
		int[] keepthesecolscr = {2};
		JavaPairRDD<Integer, Integer[]> courseRegistrationsMin = courseRegistrations.mapToPair(row -> new Tuple2<>(row._1(), filterRow(row._2(),keepthesecolscr)));
		printPartPairRDD(courseRegistrationsMin,100,"CourseRegistrations, all unneeded columns removed");
		
		// union courseOffers and courseRegistrations - done
		JavaPairRDD<Integer, Integer[]> union = courseOffersMin.union(courseRegistrationsMin);
		printPartPairRDD(union, 50, "union");
		
		// reduce by key
		// make a RDD that looks like this: key = courseOfferId, values = list of grades
		// a and b are the value objects of the pairrdd
		JavaPairRDD<Integer, Integer[]> result = union.reduceByKey((Integer[] a, Integer[] b) -> {
			/*CourseOffers has no content in tuple, v.length = 0
			* CourseRegistrations has one col in tuple, grade, v.length = 1
			* output of reduceByKey tuple should look like: sum_grade, count_courseOfferId, keep
			* after reduceByKey is done, filter to remove keep = 0
			*/

			Integer[] result_row = new Integer[3];
			for (int i = 0; i < result_row.length; i++) {
				result_row[i] = 0;
			}
			
			if (a.length == 0 && b.length == 3) {
				// A is from courseOffers, B has been processed once before
				if (b[2] == 0) {
					result_row[2] = 1;
					// set keep to true
				} else {
					result_row[2] = 0;
				}
				for (int x = 0; x < 2; x++) {
					result_row[x] = b[x];
				}
				
			}
			if (a.length == 0 && b.length == 1) {
				// A is from courseOffers, B has not been processed before
				result_row[0] = b[0]; // initialize sum_grade
				result_row[1] = 1; // initialize count
				result_row[2] = 1; // set keep to true
			}
			if (b.length == 0 && a.length == 3) {
				// B is from courseOffers, A has been processed once before
				for (int x = 0; x < result_row.length; x++) {
					result_row[x] = a[x];
				}
				if (a[2] == 0) {
					result_row[2] = 1;
					// set keep to true
				}
			}
			if (b.length == 0 && a.length == 1) {
				// B is from courseOffers, A has not been processed once before
				result_row[0] = a[0]; // initialize sum_grade
				result_row[1] = 1; // initialize count
				result_row[2] = 1; // set keep to true
			}
			if (b.length == a.length) {
				if (b.length == 0) {
					// both A and B are from courseOffers
					result_row[0] = 0;
					result_row[1] = 0;
					result_row[2] = 1;
				} else if (b.length == 1) {
					// both A and B are from courseRegistrations
					result_row[0] = a[0] + b[0]; // sum a and b
					result_row[1] += 2;
					result_row[2] = 0;
				} else if (b.length == 3) {
					// both A and B have been processed before
					result_row[0] = a[0] + b[0];
					// count does not have to be updated
					if (a[2] == 1 || b[2] == 1) {
						result_row[2] = 1;
					}
					
				}
			}
			
			return result_row;
		});
		printPartPairRDD(result, 50, "result after reducing");
		
		
		/*
		 * union = union.reduceByKey((Integer[] a, Integer[] b) -> { Integer result[] =
		 * new Integer[a.length + b.length]; for (int i = 0; i < a.length; i++) {
		 * result[i] = a[i]; } for (int i = a.length; i < (a.length+b.length); i++) {
		 * result[i] = b[i-a.length]; } return result; }); union = union.filter(tuple ->
		 * tuple._2().length > 6); printPartPairRDD(union,100,"union after reduce");
		 */


		// shutdown the spark context
		sc.close();
	}

	public static void main(String[] args) {
		// query2();
		query2alternative();
	}

}
