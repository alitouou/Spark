// Databricks notebook source
/*
Cree tres DataFrames a partir de los datos proporcionados y verifique que todos los nombres de columnas de los tres DataFrames cumplen el siguiente formato:
Todas la letras en minúscula.
Las palabras deben estar separadas por el carácter _.
Los nombres de columna no deben tener espacios en blanco al principio, final o en medio.
Los nombres de columnas no deben contener caracteres como puntos, paréntesis, o guiones medios.
Un ejemplo de como debe quedar el nombre de las columnas es el siguiente: average_price_of_1gb_usd_at_the_start_of_2021.

// COMMAND ----------

val sc = spark.sparkContext

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

/*
dbfs:/FileStore/ProyectoScalaSpark/worldwide_internet_prices_in_2022___IN_2022-1.csv
dbfs:/FileStore/ProyectoScalaSpark/worldwide_internet_speed_in_2022____avg_speed-1.csv
dbfs:/FileStore/ProyectoScalaSpark/worldwide_internet_users___users-1.csv

// COMMAND ----------

val dfInternetPrices = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/ProyectoScalaSpark/worldwide_internet_prices_in_2022___IN_2022-1.csv")

val dfInternetSpeed = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/ProyectoScalaSpark/worldwide_internet_speed_in_2022____avg_speed-1.csv")

val dfInternetUsers = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/ProyectoScalaSpark/worldwide_internet_users___users-1.csv")

// COMMAND ----------

dfInternetPrices.printSchema

dfInternetSpeed.printSchema

dfInternetUsers.printSchema

// COMMAND ----------

dfInternetUsers.show

// COMMAND ----------

val pricesCol = dfInternetPrices.columns.map(_.trim.toLowerCase.replaceAll("\\.|\\(|\\)|\\.\\–", " "))
.map(_.replaceAll(" ", "_"))
.map(_.replaceAll("__", "_"))

val speedCol = dfInternetSpeed.columns.map(_.trim.toLowerCase.replaceAll(" ", "_"))

val usersCol = dfInternetUsers.columns.map(_.trim.toLowerCase.replaceAll(" ", "_"))

// COMMAND ----------

speedCol.foreach(println)
pricesCol.foreach(println)
usersCol.foreach(println)

// COMMAND ----------

val prices = dfInternetPrices.toDF(pricesCol:_*)

val users = dfInternetUsers.toDF(usersCol:_*)

val speed = dfInternetSpeed.toDF(speedCol:_*)

// COMMAND ----------

/*Determine los cinco países con mayor número de usuarios de Internet en la región de América. La salida debe contener el nombre del país, la región, la subregión y la cantidad de usuarios de Internet.


// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

display(users)

// COMMAND ----------

val usersDF = users.withColumn("internet_users", regexp_replace(col("internet_users"), ",", "").cast(IntegerType))
                   .withColumn("population", regexp_replace(col("population"), ",", "").cast(IntegerType))

// COMMAND ----------

val paisesAmerica = usersDF.filter(col("region") === "Americas")

val top5PaisesAmerica = paisesAmerica.orderBy(desc("internet_users")).limit(5)

// COMMAND ----------

val top5PaisesAmerica = paisesAmerica.orderBy(desc("internet_users")).limit(5)

// COMMAND ----------

top5PaisesAmerica.select("country_or_area", "region", "subregion", "internet_users").show()

// COMMAND ----------

/* Obtenga el top tres de las regiones con más usuarios de internet.

// COMMAND ----------

val regiones = usersDF.groupBy("region")
  .agg(sum("internet_users").as("total_internet_users"))

val top3Regiones = regiones.orderBy(desc("total_internet_users")).limit(3)

// COMMAND ----------

top3Regiones.select("region", "total_internet_users").show()

// COMMAND ----------

/* Obtenga el país con más usuarios de Internet por región y subregión. Por ejemplo, el resultado para la región de las Américas y la subregión Norte América debería ser Estados Unidos. La salida debe contener el nombre del país con más usuarios de Internet, la región, la subregión y la cantidad de usuarios de Internet. Además, la salida debe estar ordenada de mayor a menor atendiendo a la cantidad de usuarios de Internet de cada país.


// COMMAND ----------

val regionesUsuariosInternet = usersDF.groupBy("region", "subregion")
  .agg(max("internet_users").as("most_internet_users"))


// COMMAND ----------

val paisMasUsuarios = regionesUsuariosInternet.alias("reg")
  .join(usersDF.alias("users"),
    $"reg.region" === $"users.region" &&
    $"reg.subregion" === $"users.subregion" &&
    $"reg.most_internet_users" === $"users.internet_users",
    "inner"
  )
  .select($"users.country_or_area", $"users.region", $"users.subregion", $"users.internet_users")
  .orderBy(desc("internet_users"))

// COMMAND ----------

paisMasUsuarios.show

// COMMAND ----------

/*Escriba el DataFrame obtenido en el ejercicio anterior teniendo en cuenta las siguientes cuestiones:

El DataFrame debe tener tres particiones.

La escritura del DataFrame debe quedar particionada por la región.

El modo de escritura empleado para la escritura debe ser overwrite.

El formato de escritura debe ser AVRO.

El DataFrame debe guardarse en la ruta /FileStore/ProyectoFinal/salida.

// COMMAND ----------

paisMasUsuarios.rdd.getNumPartitions


// COMMAND ----------

paisMasUsuarios.repartition(3).write.format("avro").partitionBy("region").mode("overwrite").save("/FileStore/ProyectoFinal/salida")

// COMMAND ----------

dbutils.fs.ls("/FileStore/ProyectoFinal/salida")

// COMMAND ----------

/*Determine los 10 países con la mayor velocidad promedio de internet según el test de Ookla. Es de interés conocer la región, subregión, población y usuarios de internet de cada país, por lo tanto los países a los que no se les pueda recuperar estos datos deben ser excluidos de la salida resultante.

// COMMAND ----------

display(speed)

// COMMAND ----------

speed.printSchema

// COMMAND ----------

val usersSpeed = speed.join(usersDF, col("country") === col("country_or_area"), "left")

// COMMAND ----------

usersSpeed.count

// COMMAND ----------

usersSpeed.filter(col("country_or_area").isNull).show

// COMMAND ----------

usersSpeed.filter(col("country_or_area").isNotNull)
.orderBy(desc("avg"))
.select(
  col("country"),
  col("region"),
  col("subregion"),
  col("population"),
  col("internet_users"),
  col("avg").as("avg_internet_speed")
  )
.limit(10)
.show

// COMMAND ----------

/* Determine el promedio del costo de 1GB en usd a principios del año 2021 por región. Aquellas ubicaciones a las que no pueda obtenerle la región no deben ser consideradas en el cálculo. La salida debe tener tres columnas: region, costo_prom_1_gb y grupo_region. Además, debe mostrar las regiones ordenadas de menor a mayor por su costo promedio de un 1GB en usd a principios del año 2021. La columna grupo_region debe ser etiquetada de acuerdo a la siguiente regla:

Si la región comienza con la letra A la etiqueta debe ser region_a.

Si la región comienza con la letra E la etiqueta debe ser region_e.

En los demás casos la etiqueta debe ser region_por_defecto.

// COMMAND ----------

display(prices)

// COMMAND ----------

prices.printSchema

// COMMAND ----------

val pricesDF = prices.withColumn("average_price_of_1gb_usd_at_the_start_of_2021_", regexp_replace(col("average_price_of_1gb_usd_at_the_start_of_2021_"), "\\$", "").cast(DecimalType(3,2)))
 

// COMMAND ----------

val avgCost1GB = pricesDF.join(usersDF, col("country_or_area") === col("name"), "left")

// COMMAND ----------

avgCost1GB.filter(col("region").isNull).count

// COMMAND ----------

avgCost1GB.count

// COMMAND ----------

avgCost1GB.filter(col("region").isNotNull)
.groupBy("region")
.agg(
  avg(col("average_price_of_1gb_usd_at_the_start_of_2021_")).as("costo_prom_1gb")
)
.orderBy("costo_prom_1gb")
.withColumn("grupo_region", when(col("region").startsWith("A"), lit("region_a"))
                           .when(col("region").startsWith("E"), lit("region_e"))
.otherwise(lit("region_por_defecto"))
            )
.show

// COMMAND ----------


