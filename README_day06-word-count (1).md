# Day 6 — Word Count

## Task
Implement classic Word Count (`flatMap` → `map` → `reduceByKey`), make it case-insensitive and punctuation-free, and find the top 10 most frequent words in application logs.

## Code — `src/main/scala/Day06App.scala`
```scala
import org.apache.spark.sql.SparkSession

object Day06App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Day06-WordCount").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

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
      .flatMap(_.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("Classic word count (case-sensitive, punctuation not stripped):")
    classicCounts.collect().foreach(println)

    // Case-insensitive, punctuation-stripped, empty-word-filtered version
    val cleanCounts = lines
      .flatMap(_.split("\\s+"))
      .map(_.replaceAll("[^a-zA-Z0-9]", "").toLowerCase)
      .filter(_.nonEmpty)
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
```

## Output
> Predicted — confirm by running `sbt run` and compare (word order within same count may vary since `reduceByKey` output order isn't guaranteed).
```
Classic word count (case-sensitive, punctuation not stripped):
(Spark,1)
(is,2)
(fast.,1)
(Spark,1)
(powerful!,1)
...

Cleaned word count (case-insensitive, no punctuation, no empty words):
(spark,4)
(scala,3)
(is,2)
(and,2)
(data,2)
(big,2)
(with,2)
(fast,1)
(powerful,1)

Top 10 most frequent words in logs:
(occurred,3)
(successfully,3)
(module,2)
(info,3)
(error,3)
(request,2)
...
```

## Explanation — what's happening

**1. Classic word count**
```scala
lines.flatMap(_.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)
```
- `flatMap(_.split("\\s+"))` splits every line on whitespace and flattens all lines' words into one flat RDD of words.
- `map(word => (word, 1))` turns each word into a `(word, 1)` pair — a Pair RDD.
- `reduceByKey(_ + _)` sums the `1`s for every identical key (word), giving the final count per word. This step is a shuffle since matching keys may live on different partitions.

Because punctuation wasn't stripped here, `"fast."` and `"powerful!"` count as different words from `"fast"`/`"powerful"` — this is intentional, to contrast with the cleaned version below.

**2. Cleaned, case-insensitive version**
```scala
.map(_.replaceAll("[^a-zA-Z0-9]", "").toLowerCase).filter(_.nonEmpty)
```
Strips any character that isn't a letter or digit, lowercases everything, then drops any word that became empty (e.g., a lone `"."` token). This merges `"Spark"`, `"Spark,"`, and `"SPARK"` all into the single key `"spark"`.

**3. Top 10 words in logs**
Same clean-count pipeline applied to a log file, then `.sortBy(-_._2)` sorts by count descending (negating the count sorts largest-first), and `.take(10)` returns the top 10 as an action.

## Viva Q&A
| Question | Answer |
|---|---|
| Why `reduceByKey` instead of `groupByKey` here? | `reduceByKey` combines values locally on each partition before shuffling (map-side combine), sending far less data over the network than `groupByKey`, which ships every raw value before combining. |
| What does `flatMap(_.split("\\s+"))` do differently from `map(_.split("\\s+"))`? | `map` would produce one array-per-line (nested); `flatMap` flattens all those arrays into a single flat RDD of individual words. |
| Why does `"Spark"`, `"Spark,"`, `"SPARK"` count separately in the classic version but together in the cleaned version? | The classic version does no cleaning, so punctuation and case create distinct keys; the cleaned version strips non-alphanumeric characters and lowercases before counting, merging them. |
| What triggers the shuffle in this pipeline? | `reduceByKey` — it needs identical keys (words) redistributed to the same partition to sum their counts. |
| Why filter empty strings after `replaceAll`? | A token that was pure punctuation (e.g. `"."`) becomes an empty string after stripping non-alphanumerics, and counting it as a "word" would pollute the results. |
