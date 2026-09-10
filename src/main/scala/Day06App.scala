import org.apache.spark.sql.SparkSession

object Day06App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day06-WordCount").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    // Sample text file
    val path = "words.txt"
    val pw = new java.io.PrintWriter(path)
    pw.write(
      "Spark is fast. Spark is powerful!\n" +
      "Scala and Spark, Spark and Scala.\n" +
      "Big Data with Spark, big data with SCALA.\n"
    )
    pw.close()

    val lines = sc.textFile(path)

    // Classic word count: flatMap -> map -> reduceByKey
    val classicCounts = lines
      .flatMap(_.split("\\s+"))      // split each line into words
      .map(word => (word, 1))        // pair each word with count 1
      .reduceByKey(_ + _)            // sum counts per key (shuffle happens here)

    println("Classic word count (case-sensitive, punctuation not stripped):")
    classicCounts.collect().foreach(println)

    // Case-insensitive, punctuation-stripped, empty-word-filtered version
    val cleanCounts = lines
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("[^a-zA-Z0-9]", "").toLowerCase) // strip punctuation, lowercase
      .filter(_.nonEmpty)                                 // ignore empty tokens
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("\nCleaned word count (case-insensitive, no punctuation, no empty words):")
    cleanCounts.collect().sortBy(-_._2).foreach(println)

    // Scenario: top 10 most frequent words in application logs
    val logPath = "app.log"
    val pwLog = new java.io.PrintWriter(logPath)
    pwLog.write(
      "INFO Starting application successfully\n" +
      "ERROR Null pointer exception occurred in module\n" +
      "INFO Processing request successfully\n" +
      "ERROR Timeout occurred while connecting\n" +
      "WARN Low memory detected in module\n" +
      "ERROR Disk full error occurred\n" +
      "INFO Request completed successfully\n"
    )
    pwLog.close()

    val topWords = sc.textFile(logPath)
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("[^a-zA-Z0-9]", "").toLowerCase)
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(10)

    println("\nTop 10 most frequent words in logs:")
    topWords.foreach(println)

    spark.stop()
  }
}
