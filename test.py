from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

baseDF = spark.sql("Select Distinct ChildQuestionSetCode, q.QuestionSetCode from QB_QuestionRowChoice QRC inner join QB_Question Q on Q.QuestionId = QRC.QuestionId where q.IsDeleted = 0 and QRC.IsDeleted = 0 limit 100")
baseDF.printSchema()
baseDF.createOrReplaceTempView("base_data")

def fn_QB_GetRootQuestionSetCode(QuestionSetCode):
    TempQuestionCode = QuestionSetCode
    count = 0
    while QuestionSetCode != 'null':
        if count == 5:
            break
        TempQuestionCode = 'null'
        finalDF =  spark.sql("select a.QuestionSetCode from base_data a where a.ChildQuestionSetCode = '{0}'".format(QuestionSetCode))
        if finalDF.rdd.isEmpty():
            return QuestionSetCode
        TempQuestionCode = finalDF.first()
        QuestionSetCode = TempQuestionCode
        count += 1
 
question_df = spark.sql("select QuestionSetCode from QB_Question limit 1000")
question_df.printSchema()

data_list = []

question_df_list = question_df.collect()
for i in question_df_list:
    data_list.append(tuple((i.QuestionSetCode,fn_QB_GetRootQuestionSetCode(i.QuestionSetCode))))

print(data_list)