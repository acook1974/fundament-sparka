package module3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Exercise {
  
  def exercise1(spark: SparkSession): Unit = {
    import spark.implicits._

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val jobsDF: Dataset[Row] = Seq(("programmer", 0), ("teacher", 18), ("senator", 30), ("president", 35)).toDF("job", "ageLimit")

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    val peopleWithLengthNameDF: Dataset[Row] = peopleDF.withColumn("nameLength", length(concat(col("firstName"), col("lastName"))))
    peopleWithLengthNameDF.show()

    val peopleWithJobsDF: Dataset[Row] = peopleWithLengthNameDF.join(jobsDF, peopleWithLengthNameDF("nameLength").leq(jobsDF("ageLimit")), "left")
    peopleWithJobsDF.show()
  }

  def exercise2(spark: SparkSession): Unit = {
    import spark.implicits._

    // 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)
    val netflixDF: Dataset[Row] = spark.read.option("header", "true").csv("data/netflix_titles.csv")
    println(s"--- 1. Wczytaj do Dataframe’a plik z tytułami Netflix (netflix_titles.csv)")
    netflixDF.show()

    // 2. Zbadaj strukturę danych (schema, liczba rekordów itd)
    netflixDF.printSchema()
    println(s"--- 2. Zbadaj strukturę danych (schema, liczba rekordów itd)")
    println(s"Number of netflix data: ${netflixDF.count()}")

    // 3. Zamień nulle na napisy „NULL”
    val netflixWithoutNullsDF: Dataset[Row] = netflixDF.na.fill("NULL")
    println(s"--- 3. Zamień nulle na napisy „NULL”")
    netflixWithoutNullsDF.show()

    // 4. Zbadaj ile jest filmów z podziałem na rodzaje (kolumna type)
    val netflixWithTypeDF: Dataset[Row] = netflixDF.groupBy("type")
      .agg(count(col("show_id")).as("countFilms"))
      .filter(col("type").isNotNull)
    println(s"--- 4. Zbadaj ile jest filmów z podziałem na rodzaje (kolumna type)")
    netflixWithTypeDF.show()

    // 5. Zbadaj ile tytułów nakręcili poszczególni directorzy (kolumna director)
    val netflixWithDirectorDF: Dataset[Row] = netflixDF
      .withColumn("director", split(col("director"), ","))
      .select(col("title"), explode(col("director")).as("singleDirector"))
      .withColumn("singleDirector", trim(col("singleDirector")))
      .distinct()
      .groupBy("singleDirector")
      .agg(count(col("title")).as("countFilms"))
      .orderBy(col("countFilms").desc)
    println(s"--- 5. Zbadaj ile tytułów nakręcili poszczególni directorzy (kolumna director)")
    netflixWithDirectorDF.show()

    //6. Zrób statystyki z podziałem na lata – kiedy nakręcono ile filmów (wyświetl w kolejności chronologicznej).
    val netflixWithYearDF: Dataset[Row] = netflixDF.groupBy("release_year")
      .agg(count(col("show_id")).as("countFilms"))
      .filter(regexp_extract(col("release_year"), "^\\d{4}$", 0) =!= "")
      .orderBy(col("release_year").asc)
    println(s"--- 6. Zrób statystyki z podziałem na lata – kiedy nakręcono ile filmów (wyświetl w kolejności chronologicznej).")
    netflixWithYearDF.show()

    //7. Określ ile jest filmów przypisanych do poszczególnych gatunków (listed_in)
    val listedInStatsDF: Dataset[Row] = netflixDF.withColumn("listed_in", split(col("listed_in"), ","))
      .select(col("show_id"), explode(col("listed_in")).as("singleListedIn"))
      .withColumn("singleListedIn", trim(col("singleListedIn")))
      .groupBy("singleListedIn")
      .agg(count(col("show_id")).as("countFilms"))
      .orderBy(col("singleListedIn").asc)
    println(s"--- 7. Określ ile jest filmów przypisanych do poszczególnych gatunków (listed_in)")
    listedInStatsDF.show(truncate = false)
  }

  def exercise3(spark: SparkSession): Unit = {
    import spark.implicits._

    // 1. Wczytaj do Dataframe’a plik z danymi o pizzach (pizza_data.csv)
    val pizzaDF: Dataset[Row] = spark.read.option("header", "true").csv("data/pizza_data.csv")
    println(s"--- 1. Wczytaj do Dataframe’a plik z danymi o pizzach (pizza_data.csv)")
    pizzaDF.show()
    println(s"Number of pizza data: ${pizzaDF.count()}")

    // 2. Zbadaj strukturę danych (schema, liczba rekordów itd)
    println(s"--- 2. Zbadaj strukturę danych (schema, liczba rekordów itd)")
    pizzaDF.printSchema()

    // 3. Wybierz średnie pizze i oblicz średnią, minimalną i maksymalną cenę
    var mediumPizzaDF: Dataset[Row] = pizzaDF.filter(col("Size").contains("Medium"))
      .filter(col("Price").isNotNull)
      .withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast(DoubleType))
      .agg(avg(col("Price")).as("avgPrice"), min(col("Price")).as("minPrice"), max(col("Price")).as("maxPrice"))
    mediumPizzaDF.show()

    mediumPizzaDF.printSchema()
  }

}
