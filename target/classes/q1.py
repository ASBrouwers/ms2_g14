'''
Created on Mar 15, 2019

@author: student
'''
import findspark
findspark.init("/home/student/spark")
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder.master("local").appName("test1").config("spark.some.config.option", "some-value").getOrCreate()
sqlContext = SQLContext(spark)

schemaCO = StructType([
    StructField("COId", IntegerType(), True),
    StructField("CourseId", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Quartile", IntegerType(), True)])

schemaCR = StructType([
    StructField("CourseOfferId", IntegerType(), True),
    StructField("StudentRegistrationId", IntegerType(), True),
    StructField("Grade", IntegerType(), True)])

CourseOffers = spark.read.csv("/home/student/tables2/tables/CourseOffers.table", header=False, schema=schemaCO)
CourseRegistrations = spark.read.csv("/home/student/tables2/tables/CourseRegistrations.table", header=False, schema=schemaCR)

joinResult = CourseOffers.join(CourseRegistrations, CourseOffers.COId == CourseRegistrations.CourseOfferId)

joinResult.createOrReplaceTempView("CO")

q11 = sqlContext.sql("SELECT CourseOfferId, AVG(Grade) AS AverageGrade FROM CO WHERE Year = 2016 AND Quartile = 2 AND Grade >= 5 GROUP BY CourseOfferId")
q11.show()

q12 = sqlContext.sql("SELECT AVG(Grade) AS AverageGrade FROM CO WHERE StudentRegistrationId = 3 AND Grade >= 5")
q12.show()

spark.stop()


