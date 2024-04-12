# Databricks notebook source
# DBTITLE 1,Using CreateDataFrame
data = [("Utsav", "Singh", "23", "22-07-2000", "M", 200000),
        ("Sourav", "Goyal", "24", "26-09-1999", "M", 400000),
        ("Vaishnavi","Choudhary", "25", "18-09-1998", "F", 5000),
        ("Gauri","Sharma","26", "02-06-1997", "F", 8000),
        ("Akash","Dua", "27", "20-11-1996", "M", 6000)]
columns = ["FirstName","LastName","Age","DOB","Gender","Salary"]
df = spark.createDataFrame(data=data, schema=columns)    
df.show()

# COMMAND ----------

# DBTITLE 1,Reading a File & display
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/utsavsingh62@gmail.com/Order-1.csv")
df1.display()

# COMMAND ----------

# DBTITLE 1,Creating Temp View
df.createOrReplaceTempView("Person_data")
df2 = spark.sql("Select * FROM person_data")
df2.printSchema()
df2.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT firstname,salary,age,count(*) FROM person_data GROUP BY firstname,salary,age

# COMMAND ----------


