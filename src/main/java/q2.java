
public class q2 {
	
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
	 * */
	
	public static void query2() {
		System.out.print("hello world");
	}

	public static void main(String[] args) {
		query2();
	}

}
