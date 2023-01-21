import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD


object Part2 {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: InputDir OutputDir")
    }

    val spark = SparkSession.builder.appName("Social Network Analysis").getOrCreate()

    Logger.getLogger("Part1").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    // Loading Data
    import spark.implicits._

    val slash_dot = spark.read.option("header", "true").option("delimiter", "\t").csv("s3://qwertyu2323/Slashdot0902.txt")

    // Creating vertices, edges and Graph
    val vertices = slash_dot.select("FromNodeId").union(slash_dot.select("ToNodeId")).distinct().toDF("id")
    val edges = slash_dot.withColumnRenamed("FromNodeId", "src").withColumnRenamed("ToNodeId", "dst")

    val verticesRDD: RDD[(VertexId, String)] = vertices.select(vertices("id").cast("long"), vertices("id")).rdd.map(row => (row.getLong(0), row.getString(1)))
    val edgesRDD: RDD[Edge[Int]] = edges.select(edges("src").cast("int"), edges("dst").cast("int")).rdd.map(row => Edge(row.getInt(0), row.getInt(1), 1))

    val graph: Graph[String, Int] = Graph(verticesRDD, edgesRDD)
    graph.cache()

    // Running Queries

    var output = ""
    output += "Results of the queries are as follows: \n"

    // a. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each
    val outDegree = graph.outDegrees.sortBy(-_._2).take(5)
    output += "\nTop 5 nodes with the highest outdegree and their corresponding number of outgoing edges -\n"
    outDegree.foreach {x => output += (x._1 + " has " + x._2 + " out degree.\n")}

    // b. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges in each
    val inDegree = graph.inDegrees.sortBy(-_._2).take(5)
    output += "\nTop 5 nodes with the highest indegree and their corresponding number of incoming edges -\n"
    outDegree.foreach {x => output += (x._1 + " has " + x._2 + " in degree.\n")}


    // c. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank values. You are free to define the threshold parameter.
    val ranks = graph.pageRank(0.1).vertices
    val topVertices = ranks.sortBy(-_._2).take(5)
    output += "\nTop 5 nodes having highest pagerank values -\n"
    topVertices.foreach{x => output += (x._1 + " node has " + x._2 + " pagerank value.\n")}

    // d. Run the connected components algorithm on it and find the top 5 components with the largest number of nodes.
    val id = graph.stronglyConnectedComponents(numIter = 10).vertices.map((v: (Long, Long)) => v._2)
    val cc_id = id.map((_, 1L)).reduceByKey(_ + _)
    val topComponents = cc_id.sortBy(-_._2).take(5)
    output += "\nTop 5 components with the largest number of nodes -\n"
    topComponents.foreach{x => output += (x._1 + "'th connected component has " + x._2 + " nodes.\n")}

    // e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the largest triangle count. In case of ties, you can randomly select the top 5 vertices.
    val triangle_count = graph.triangleCount().vertices
    val topTriangleVertices = triangle_count.sortBy(-_._2).take(5)
    output += "\nTop 5 vertices with the largest triangle count -\n"
    topTriangleVertices.foreach{x => output += (x._1 + " node has " + x._2 + " triangles passing through it.\n")}


    val output_rdd = spark.sparkContext.parallelize(Seq(output))
    output_rdd.saveAsTextFile("Outputfile")

  }

}
