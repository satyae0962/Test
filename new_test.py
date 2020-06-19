from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

baseDF = spark.sql("Select Distinct ChildQuestionSetCode, q.QuestionSetCode from QB_QuestionRowChoice QRC inner join QB_Question Q on Q.QuestionId = QRC.QuestionId where q.IsDeleted = 0 and QRC.IsDeleted = 0 limit 100")
baseDF.printSchema()
baseDF.createOrReplaceTempView("base_data")

question_df = spark.sql("select QuestionSetCode from QB_Question limit 1000")
question_df.createOrReplaceTempView("question_data")
question_df.printSchema()

run_limit = 5

for i in run_limit:
        finalDF =  spark.sql("select a.QuestionSetCode from base_data a where a.ChildQuestionSetCode = '{0}'"))
        