import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd._

case class Article2(val title: String, val body: String)

object WikiPageRankSpark {
   
  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis()
    var master = args(0).trim
    var dumpPath = args(1).trim
    
    if((master == null) || (master.length() < 1)) {
      println("Arg0 missing. Mention 'local' or master url")
      return
    }  
    
    if((dumpPath == null) || (dumpPath.length() < 1)) {
      println("Arg1 missing. Please specify the dump file location!")
      return
    }  
    
    val iters = 10
    val conf = new SparkConf().setAppName("WikiPageRankSpark").setMaster(master)
    val sc = new SparkContext(conf)
    val wiki: RDD[String] = sc.textFile(dumpPath).coalesce(20)
    
    // Parse the articles
    val articles = wiki.map(_.split('\t')).
    filter(line => (line.length > 1 && !(line(1) contains "REDIRECT"))).
    map(line => new Article2(line(1).trim, line(3).trim)).cache
    
    // Generate (srcVertex, dstVertext) tuples RDD
    val pattern = "<target>.+?</target>".r
    val edges = articles.flatMap { a =>
      val srcVid = a.title
      pattern.findAllIn(a.body).map { link =>
        val dstVid = link.replace  ("<target>", "").replace("</target>", "")
        (srcVid, dstVid)
      }
    }.distinct().groupByKey().cache    
    
    // Give default 1.0 rank to every vertex
    var ranks = edges.mapValues(v => 1.0) // eg: (Anarchy, 1.0)
    
    for (i <- 1 to iters) {
      val contribs = edges.join(ranks).values.flatMap { case (vertices, rank) =>
        val size = vertices.size
        vertices.map(vertex => (vertex, rank / size))  
      }
      
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    
    // sort the output by rank
    val output = ranks.sortBy(x => x._2, false)
    
    // print top 100 ranks
    output.take(100).foreach(tup => println(tup._1 + ": " + tup._2))
    
    sc.stop()
    
    val t2 = System.currentTimeMillis()
    println("Running Time -> " + ((t2 - t1) / 1000) + " seconds")    
  }
}
