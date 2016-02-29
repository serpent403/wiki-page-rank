import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd._

case class Article(val title: String, val body: String)

object WikiPageRankGraphx {
  
  // Hash function to assign an Id to each article
  def pageHash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }
   
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
    val conf = new SparkConf().setAppName("WikiPageRankGraphx").setMaster(master)
    val sc = new SparkContext(conf)
    val wiki: RDD[String] = sc.textFile(dumpPath).coalesce(20)
    
    // Parse the articles
    val articles = wiki.map(_.split('\t')).
    filter(line => (line.length > 1 && !(line(1) contains "REDIRECT"))).
    map(line => new Article(line(1).trim, line(3).trim)).cache
    
    // Generate vertices with id and article title
    val vertices = articles.map(a => (pageHash(a.title), a.title)).cache    
    
    // Generate edges (srcVertex, dstVertex, 1.0)
    val pattern = "<target>.+?</target>".r
    val edges: RDD[Edge[Double]] = articles.flatMap { a =>
      val srcVid = pageHash(a.title)
      pattern.findAllIn(a.body).map { link =>
        val dstVid = pageHash(link.replace  ("<target>", "").replace("</target>", ""))
        Edge(srcVid, dstVid, 1.0)
      }
    }    
    
    // Graph[VD, ED] -> Graph[title: String, edgeWeight: Double]
    val graph = Graph(vertices, edges, "").subgraph(vpred = {(v, d) => d.nonEmpty}).cache
    val prGraph = graph.staticPageRank(iters).cache()
    
    // Combine title information with the page ranks
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
    
    // fetch top 100 articles (vertices)
    titleAndPrGraph.vertices.top(100) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))

    sc.stop() 
    
    val t2 = System.currentTimeMillis()
    println("Running Time -> " + ((t2 - t1) / 1000) + " seconds")
  }  
}


