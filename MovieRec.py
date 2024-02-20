# Databricks notebook source
adlsAccountName="sskstoragelearning"
adlsContainername="validated"
adlsFolderName="Data"
mountpoint="/mnt/Files/validated"
applicationId=dbutils.secrets.get(scope="moverecscp1",key="clientappid")
authenticationKey=dbutils.secrets.get(scope="moverecscp1",key="clientsecretval")
tenantId=dbutils.secrets.get(scope="moverecscp1",key="clienttenantid")
endpoint="https://login.microsoftonline.com/"+tenantId+"/oauth2/token"
source="abfss://"+adlsContainername+"@"+adlsAccountName+".dfs.core.windows.net/"+adlsFolderName
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": applicationId,
          "fs.azure.account.oauth2.client.secret": authenticationKey,
          "fs.azure.account.oauth2.client.endpoint": endpoint}
#Mounting ADLS storage to DBFS
#mount if the directory is not already mounted 
if not any(mount.mountPoint==mountpoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source=source,
        mount_point=mountpoint,
        extra_configs=configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/Files/validated')

# COMMAND ----------

movies_filename='dbfs:/mnt/Files/validated/movies.csv'
rating_filename='dbfs:/mnt/Files/validated/ratings.csv'

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/Files/validated/

# COMMAND ----------

from pyspark.sql.types import *
#working on movies.csv file
movies_with_genres_df_schema = StructType(
[StructField('ID',IntegerType()),
 StructField('title',StringType()),
 StructField('genres',StringType())]
)

movies_df_schema = StructType(
   [StructField('ID',IntegerType()),
    StructField('title',StringType())]
)


# COMMAND ----------

movies_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferSchema=False).schema(movies_df_schema).load(movies_filename)
movies_with_genres_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferSchema=False).schema(movies_with_genres_df_schema).load(movies_filename)

# COMMAND ----------

# MAGIC %md
# MAGIC #inspecting the dataframe before the transformation

# COMMAND ----------

movies_df.show(4,truncate=False)
movies_with_genres_df.show(4,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Rating data Analysis

# COMMAND ----------

from pyspark.sql.types import *

ratings_df_schema = StructType(
[StructField('userId',IntegerType()),
 StructField('moveId',IntegerType()),
 StructField('rating',DoubleType())]
)

ratings_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True,inferSchema=False).schema(ratings_df_schema).load(rating_filename)
ratings_df.show(4,truncate=False)

# COMMAND ----------

movies_df.cache()
ratings_df.cache()

# COMMAND ----------

from pyspark.sql import functions as F
movie_ids_with_avg_ratings_df=ratings_df.groupBy('moveId').agg(F.count(ratings_df.rating).alias("count"),F.avg(ratings_df.rating).alias("average"))
print("movie_ids_with_avg_ratings_df:")
movie_ids_with_avg_ratings_df.show(4,truncate=False)

# COMMAND ----------

movie_names_with_avg_rating_df=movie_ids_with_avg_ratings_df.join(movies_df,F.col("MoveId") == F.col("ID")).drop('ID')
movie_names_with_avg_rating_df.show(4,truncate=False)

# COMMAND ----------

movies_with_500_ratings_or_more = movie_names_with_avg_rating_df.filter(movie_names_with_avg_rating_df['count'] >= 500).orderBy('average',acending=False)
movies_with_500_ratings_or_more.show(4,truncate=False)


# COMMAND ----------

from pyspark.sql import functions as F
seed=4
(split_60_df,split_a_20_df,split_b_20_df) = ratings_df.randomSplit([0.6,0.2,0.2],seed)

training_df = split_60_df.cache()
validation_df = split_a_20_df.cache()
test_df = split_b_20_df.cache()

print('Training: {0},Validation: {1},test: {2}\n'.format(
    training_df.count(), validation_df.count(), test_df.count())
)

training_df.show(4,truncate=False)
validation_df.show(4,truncate=False)
test_df.show(4,truncate=False)

# COMMAND ----------

from pyspark.ml.recommendation import ALS
als = ALS()

als.setPredictionCol("prediction")\
    .setMaxIter(5)\
    .setSeed(seed)\
    .setRegParam(0.1)\
    .setUserCol("userId")\
    .setItemCol("moveId")\
    .setRatingCol("rating")\
    .setRank(8)

my_ratings_model = als.fit(training_df)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col

reg_eval = RegressionEvaluator(predictionCol="prediction",labelCol="rating",metricName="rmse")
my_predict_df=my_ratings_model.transform(test_df)

predicated_test_my_ratings = my_predict_df.filter(my_predict_df.prediction != float('nan'))

test_RMSE_my_ratings = reg_eval.evaluate(predicated_test_my_ratings)
print('The model had a RMSE on the test set of {0}'.format(test_RMSE_my_ratings))

dbutils.widgets.text("input","5","")
ins = dbutils.widgets.get("input")
uid=int(ins)
ll=predicated_test_my_ratings.filter(col("userId")==uid)


# COMMAND ----------

MovieRec = ll.join(movies_df,F.col("moveId") == F.col("ID")).drop('ID').select('title').take(10)
l=dbutils.notebook.exit(MovieRec)
