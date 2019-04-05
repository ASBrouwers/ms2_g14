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
 * 		3. union the pair RDDs - done
 * 		4. reducebykey -done
 * 			* create cartesian product - where multiple rows are stored in one tuple
 * 			* use flatmap to split the cartesian product into single rows
 * 			* filter rows on courseofferid columns match.
 * */

public class q2old {
	static int take_var = 20;
	static boolean show_intermediate_rdds = false;

	public static JavaRDD<String> removeHeader(JavaRDD<String> inputRDD) {
		String header = (String) inputRDD.first();
		inputRDD = inputRDD.filter(row -> !row.equals(header));
		return inputRDD;
	}

	public static JavaRDD<String> addIdentifier(JavaRDD<String> input, String id) {
		JavaRDD<String> output = input.map(row -> row += "," + id);
		return output;
	}
	
	public static boolean rowContains(String[] row, String id) {
		boolean result = false;
		for (String r : row) {
			if (r.contentEquals(id)) {
				result = true;
			}
		}
		return result;
	}
	
	public static String[] removeUnneededCols(String[] row, int[] keep) {
		List<String> result_list = new ArrayList<String>();
		for (int i = 0; i < row.length; i++) {
			for (int k = 0; k < keep.length; k++) {
				if (i == keep[k]) {
					result_list.add(row[i]);
				}
			}
		}
		
		String[] result = new String[keep.length];
		for (int i = 0; i < result_list.size(); i++) {
			result[i] = result_list.get(i);
		}
		return result;
	}

	public static void printPartPairRDD(JavaPairRDD<Integer, String[]> input, int limit, String description) {
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

	public static void printPartPairListRDD(JavaPairRDD<Integer, ArrayList<String[]>> input, int limit, String description) {
		int actual_size = (int) input.count();
		System.out.println(description + "\n RDD has size " + actual_size + "\n");
		if (actual_size < limit) {
			limit = actual_size;
			System.out.println("PairRDD length is smaller than limit parameter");
			input.foreach(row -> {
				String output = "key: " + row._1() + ", values: ";
				for (int i = 0; i < row._2().size(); i++) {
					System.out.println("size of row._2(): " + row._2().size());
					for (int j = 0; j < row._2().get(i).length; j++) {
						output += row._2().get(i)[j] + ", ";
					}
					output += "\n";
				}
				System.out.println(output);
			});
		} else {
			input.take(limit).forEach(row -> {
				String output = "key: " + row._1() + ", values: ";
				for (int i = 0; i < row._2().size(); i++) {
					for (int j = 0; j < row._2().get(i).length; j++) {
						output += row._2().get(i)[j] + ", ";
					}
					output += "\n";
				}
				System.out.println(output);
			});
		}
	}

	
	public static void query2(int courseId, int quartile, int year) {
		String startingPath; // Folder where table data is located
		startingPath = "/tmp/tables/";
		//startingPath = "/tmp/tables_reduced/";
		String master = "local[2]"; // Run locally with 1 thread

		// Setup Spark
		SparkConf conf = new SparkConf().setAppName(q2old.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
		JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
		JavaRDD<String> textCourseOffersId = addIdentifier(textCourseOffers, "CO");
		JavaRDD<String> textCourseRegistrationsId = addIdentifier(textCourseRegistrations, "CR");

		// first column is the PK
		JavaPairRDD<Integer, String[]> courseOffers = textCourseOffersId
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), row.split(",")));
				
		// first column is the PK
		JavaPairRDD<Integer, String[]> courseRegistrations = textCourseRegistrationsId
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), row.split(",")));
				
		// filter courseOffers - done
		// keep CourseId = var, CourseId is stored in the 2nd column
		// keep Quartile = var, Quartile is stored in the 4th column
		// keep Year = var, Year is stored in the 3rd column
		if (show_intermediate_rdds) {
			printPartPairRDD(courseOffers, take_var, "CourseOffers, prior to filtering");
		}
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[1]) == courseId);
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[3]) == quartile);
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[2]) == year);
		if (show_intermediate_rdds)  {
			printPartPairRDD(courseOffers, take_var, "CourseOffers, after filtering");
		}

		// filter courseRegistrations - done
		if (show_intermediate_rdds) {
			printPartPairRDD(courseRegistrations, take_var, "CourseRegistrations, prior to filtering");
		}
		// remove grade = "null"
		courseRegistrations = courseRegistrations.filter(tuple -> !tuple._2()[2].contentEquals("null"));
		// keep grade >= 5
		courseRegistrations = courseRegistrations.filter(tuple -> Integer.parseInt(tuple._2()[2]) >= 5);
		if (show_intermediate_rdds)  {
			printPartPairRDD(courseRegistrations, take_var, "CourseRegistrations, after filtering");
		}

		// remove unneeded columns	
		int[] keepthesecolscr = {2};
		JavaPairRDD<Integer, String[]> courseRegistrationsMin = courseRegistrations
				.mapToPair(row -> new Tuple2<>(row._1(), removeUnneededCols(row._2(),keepthesecolscr)));

		int[] keepthesecolsco = {(courseOffers.first()._2().length-1)};
		JavaPairRDD<Integer, String[]> courseOffersMin = courseOffers
				.mapToPair(row -> new Tuple2<>(row._1(), removeUnneededCols(row._2(),keepthesecolsco)));
		
		
		
		// union courseOffers and courseRegistrations - done
		JavaPairRDD<Integer, String[]> union = courseOffersMin.union(courseRegistrationsMin);
		if (show_intermediate_rdds) {
			printPartPairRDD(union, take_var, "union");
		}
		
		// reduce by key
		// make a RDD that looks like this: key = courseOfferId, values = list of grades
		// a and b are the value objects of the pairrdd
		// reducebykey -> perform cartesian product - store multiple rows in a single tuple
		// then, use flatmap to split the composite rows into several rows.		
		
		union = union.reduceByKey((a,b) -> {
			String[] result = new String[a.length+b.length];
			for (int i = 0; i < a.length; i++) {
				result[i] = a[i];
			}
			for (int i = a.length; i < a.length+b.length; i++) {
				result[i] = b[i-a.length];
			}
			return result;
		});
		if (show_intermediate_rdds || true) {
			printPartPairRDD(union, take_var, "union after running reducebykey");
		}
		
		// union now only contains the row with the grades of the desired courseOfferId
		union = union.filter(row -> rowContains(row._2(),"CO") == true);
		if (show_intermediate_rdds) {
			printPartPairRDD(union, take_var, "union that only contains matching courseOfferIds");
		}
		
		// use flatmap to split the remaining row in [union] into new tuples of a new rdd
		JavaPairRDD<Integer, String[]> grades = union.flatMapToPair(tuple -> {
			List<Tuple2<Integer,String[]>> out = new ArrayList<>();
			for (String x : tuple._2()) {
				if (!x.contentEquals("CO")) {
				String[] val = {x};
				out.add(new Tuple2<>(tuple._1(),val));
				}
			}
			return out.iterator();
		});
		if (show_intermediate_rdds) {
			printPartPairRDD(grades, take_var, "result after flatMapToPair");
		}
		
		// use rdd.count() to determine the count for the average calculation
		float count = grades.count();
		
		// use reducebykey to calculate the sum of all grades in [grades]
		JavaPairRDD<Integer,String[]> sum_grade = grades.reduceByKey((a,b) -> {
			int part_sum = Integer.parseInt(a[0]) + Integer.parseInt(b[0]);
			String out_s = part_sum + "";
			String[] out = {out_s};
			return out;
		});
		if (show_intermediate_rdds) {
			printPartPairRDD(sum_grade, take_var, "sum of all grades for selected courseOfferId");
		}
		
		// store sum of [grades] in Java variable and calculate and print the average.
		if (sum_grade.count() == 1) {
			float sum = Integer.parseInt(sum_grade.first()._2()[0]);
			float average = sum/count;
			System.out.println("Average grade: " + average + ", for CourseId = " + courseId + ", Quartile = " + quartile + ", Year = " + year + ", and all grades >= 5");
		} else {
			System.out.println("error - the final RDD contains too many rows to complete the computation");
		}
		
		// shutdown the spark context
		sc.close();
	}
	
	/* ** reworked equi - join ** */
	public static String query2alternative(int courseId, int quartile, int year) {
		String func_result = "";
		String courseOffersTableId = "CO";
		String courseRegistrationsTableId = "CR";
		
		String startingPath; // Folder where table data is located
		startingPath = "/tmp/tables/";
		//startingPath = "/tmp/tables_reduced/";
		String master = "local[4]"; // Run locally with 4 threads

		// Setup Spark
		SparkConf conf = new SparkConf().setAppName(q2old.class.getName()).setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> textCourseOffers = removeHeader(sc.textFile(startingPath + "CourseOffers.table"));
		JavaRDD<String> textCourseRegistrations = removeHeader(sc.textFile(startingPath + "CourseRegistrations.table"));
		//textCourseRegistrations = textCourseRegistrations.sample(false, 0.25);
		JavaRDD<String> textCourseOffersId = addIdentifier(textCourseOffers, courseOffersTableId);
		JavaRDD<String> textCourseRegistrationsId = addIdentifier(textCourseRegistrations, courseRegistrationsTableId);

		// first column is the PK
		JavaPairRDD<Integer, String[]> courseOffers = textCourseOffersId
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), row.split(",")));
				
		// first column is the PK
		JavaPairRDD<Integer, String[]> courseRegistrations = textCourseRegistrationsId
				.mapToPair(row -> new Tuple2<>(Integer.valueOf(row.split(",")[0]), row.split(",")));
		/* ** FILTERING ** */		
		// filter courseOffers - done
		// keep CourseId = var, CourseId is stored in the 2nd column
		// keep Quartile = var, Quartile is stored in the 4th column
		// keep Year = var, Year is stored in the 3rd column
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[1]) == courseId);
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[3]) == quartile);
		courseOffers = courseOffers.filter(tuple -> Integer.parseInt(tuple._2()[2]) == year);
		// filter courseRegistrations - done
		// remove grade = "null"
		courseRegistrations = courseRegistrations.filter(tuple -> !tuple._2()[2].contentEquals("null"));
		// keep grade >= 5
		courseRegistrations = courseRegistrations.filter(tuple -> Integer.parseInt(tuple._2()[2]) >= 5);
		
		// remove unneeded columns	
		int[] keepthesecolscr = {2,(courseRegistrations.first()._2().length-1)};
		JavaPairRDD<Integer, String[]> courseRegistrationsMin = courseRegistrations
				.mapToPair(row -> new Tuple2<>(row._1(), removeUnneededCols(row._2(),keepthesecolscr)));

		int[] keepthesecolsco = {(courseOffers.first()._2().length-1)};
		JavaPairRDD<Integer, String[]> courseOffersMin = courseOffers
				.mapToPair(row -> new Tuple2<>(row._1(), removeUnneededCols(row._2(),keepthesecolsco)));
		
		JavaPairRDD<Integer,String[]> union = courseOffersMin.union(courseRegistrationsMin);
		
		// create partial cartesian
		JavaPairRDD<Integer, ArrayList<Tuple2<Integer,String[]>>> unionList = union.mapToPair(tuple ->{
			ArrayList<Tuple2<Integer,String[]>> list = new ArrayList<>();
			list.add(tuple);
			Tuple2<Integer, ArrayList<Tuple2<Integer,String[]>>> result = new Tuple2<>(tuple._1, list);
			return result;
		});
		
		JavaPairRDD<Integer, ArrayList<Tuple2<Integer,String[]>>> preJoin = unionList.reduceByKey((a,b) -> {
			ArrayList<Tuple2<Integer, String[]>> list = a;
        	list.addAll(b);
        	return list;
		});
		
		JavaPairRDD<Integer,String[]> join = preJoin.flatMapToPair(tuple -> {
			ArrayList<Tuple2<Integer,String[]>> all = tuple._2;		
			ArrayList<Tuple2<Integer,String[]>> courseOffersList = new ArrayList<>();
			ArrayList<Tuple2<Integer,String[]>> courseRegistrationsList = new ArrayList<>();	
			for (Tuple2<Integer,String[]> i : all) {
				if (i._2()[i._2().length-1].contentEquals(courseOffersTableId)) {
					courseOffersList.add(i);
				} else {
					courseRegistrationsList.add(i);
				}
			}
			//System.out.println("size of courseRegistrationsList: " + courseRegistrationsList.size());
			ArrayList<Tuple2<Integer,String[]>> out = new ArrayList<>();
			for (Tuple2<Integer,String[]> courseOffer : courseOffersList) {
				for (Tuple2<Integer,String[]> courseRegistration : courseRegistrationsList) {
					if (courseOffer._1 == courseRegistration._1) {
						String[] content = new String[courseOffer._2().length + courseRegistration._2().length];
						for (int i = 0; i < courseOffer._2().length; i++) {
							content[i] = courseOffer._2()[i];
						}
						for (int i = courseOffer._2().length; i < courseOffer._2().length + courseRegistration._2().length; i++) {
							content[i] = courseRegistration._2()[i];
						}
						out.add(new Tuple2<>(courseOffer._1(),content));
					}
				}
			}	
			return out.iterator();
		});
		printPartPairRDD(join, take_var, "output from reworked join");
		
		// reduceByKey to obtain sum and count
				
		
		

		// shutdown the spark context
		sc.close();
		return func_result;
	}

	public static void main(String[] args) {
		/*int courseId = 18937;
		int quartile = 4;
		int year = 2007;
		query2(courseId, quartile, year);*/
		
		String test_results = "** Results ** \n";
		test_results += query2alternative(1127,3,2007) + "\n";
		//test_results += query2alternative(23246,4,2017) + "\n";
		//test_results += query2alternative(12457,2,2014) + "\n";
		//test_results += query2alternative(31814,4,2015) + "\n";
		System.out.println(test_results);
		
		/*
		 * ** tests using initial query2(), using full tables**
		 * randomly selected entries from CourseOffers
		 * courseId = 1127
		 * quartile = 3
		 * year = 2007
		 * average grade spark = 6.365217
		 * average grade sql   = 6.3652173913043478
		 * 
		 * courseId = 23246
		 * quartile = 4
		 * year = 2017
		 * average grade spark = 5.5972223
		 * average grade sql   = 5.5972222222222222
		 * 
		 * courseId = 12457
		 * quartile = 2
		 * year = 2014
		 * average grade spark = 7.060606
		 * average grade sql   = 7.0606060606060606
		 * 
		 * courseId = 31814
		 * quartile = 4
		 * year = 2015
		 * average grade spark = 6.424779
		 * average grade sql   = 6.4247787610619469
		 * */
	}

}
