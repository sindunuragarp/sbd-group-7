import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Bacon {
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the actors.list file
	final val Infinite = 100
	final val Distance = 6
	final val compressRDDs = true
	final val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/sparkLog.txt"), "UTF-8"))

	////

	def getActor2MoviesList (text: String, gender: Boolean) : ((String, Boolean), Array[String]) = {
		val data = text
      .split('\t')
      .map(x => x.replace("\n", ""))
      .filter(x => x != "")

    val actor = data(0)
		val movie_list = data.slice(1, data.length)
      .filter(x => x(0) != '"')
      .map(x => removeAfterDate(x))
      .filter {
        x =>
        try {
          val s = x.indexOf('(')
          val e = x.indexOf(')')
          val ss = x.slice(s+1, e)
          ss.slice(0,4).toInt >= 2011 // All movies in this decade (2011 and onwards, that is).
			  }
        catch {
          case e: Exception => false
        }
      }

		if (movie_list.length != 0)
			return ((actor, gender), movie_list)
		else
			return null
	}

	def getActor2Movie (x: (Long, Array[String])) : Array[(Long, String)] = {
		val actor = x._1
		val movie_list = x._2
		val a2m = ArrayBuffer[(Long, String)]()

		for (movie <- movie_list)
			a2m.append((actor, movie))

		return a2m.toArray
	}

	def updateDistance(x: (Long, (Int, Iterable[Long])), dist: Int): Array[(Long, Int)] = {
		//(actor, (distance, actorlist))
		val actor = x._1
		val distanceAndLOA = x._2
		val distance = distanceAndLOA._1
		val actorList = distanceAndLOA._2
		val ad = ArrayBuffer[(Long, Int)]()

		if (distance == dist)
		{
			ad.append((actor, distance))
			for(a <- actorList)
				ad.append((a, distance + 1))
		}
		else
			ad.append((actor, distance))

		return ad.toArray
	}

  def removeAfterDate(s: String) : String = {
    return s.substring(0, s.indexOf(')') + 1)
  }

	def getTimeStamp: String = {
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime) + "] "
	}

  ////

	def main(args: Array[String]) {
		val cores = args(0)
		val inputFileM = args(1)
		val inputFileF = args(2)

		println(inputFileM)
		println(inputFileF)

		if (!new File(inputFileM).exists) {
			println("The file for male actors: " + inputFileM + ", does not exist!")
			System.exit(1)
		}

		if (!new File(inputFileF).exists) {
			println("The file for female actors: " + inputFileF + ", does not exist!")
			System.exit(1)
		}

		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		if (compressRDDs) conf.set("spark.rdd.compress", "true")
		val sc = new SparkContext(conf)

    println("Number of cores: " + args(0))
    println("Input files: " + inputFileM + " and " + inputFileF)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


		/////////////////////////////////////////////////////////////////////////////////////////////////


		sc.addSparkListener(new SparkListener() {
			override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
				bw.write(getTimeStamp + " Spark ApplicationStart: " + applicationStart.appName + "\n")
				bw.flush()
			}

			override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
				bw.write(getTimeStamp + " Spark ApplicationEnd: " + applicationEnd.time + "\n")
				bw.flush()
			}

			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
				val map = stageCompleted.stageInfo.rddInfos
				map.foreach(row => {
					if (row.isCached) {
						bw.write(getTimeStamp + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " +
							row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n")
					}
					else if (row.name.contains("rdd_"))
						bw.write(getTimeStamp + row.name + " processed!\n")
					bw.flush()
				})
			}
		})


    /////////////////////////////////////////////////////////////////////////////////////////////////


    var t0 = System.currentTimeMillis
    val c = new Configuration(sc.hadoopConfiguration)
    c.set("textinputformat.record.delimiter", "\n\n")

    val inputM = sc.newAPIHadoopFile(inputFileM, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], c).coalesce(cores.toInt / 2)
    val inputF = sc.newAPIHadoopFile(inputFileF, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], c).coalesce(cores.toInt / 2)


    /////////////////////////////////////////////////////////////////////////////////////////////////


    // ((actor, gender), movielist)
    val actor2ListOfMoviesM = inputM
      .map(x => getActor2MoviesList(x._2.toString, gender = true))
      .filter(x => x != null)

    val actor2ListOfMoviesF = inputF
      .map(x => getActor2MoviesList(x._2.toString, gender = false))
      .filter(x => x != null)

    // ((actor, movielist), i)
    val actor2ListOfMoviesAndId = actor2ListOfMoviesM.union(actor2ListOfMoviesF)
      .zipWithIndex
      .map{case(((actor, gender), movies), i) =>
        if (gender)
          ((actor, movies), i)
        else
          ((actor, movies), -1*(i+1))
      }

    actor2ListOfMoviesAndId.setName("rdd_actor2ListOfMoviesAndId")
    actor2ListOfMoviesAndId.persist(if (compressRDDs) MEMORY_ONLY_SER else MEMORY_ONLY)
    val kevinBacon = actor2ListOfMoviesAndId.filter(x => x._1._1 == KevinBacon).first()
    val KevinBaconID = kevinBacon._2

    // (actor, movielist)
    val actorId2ListOfMovies = actor2ListOfMoviesAndId.map{case((a, ml), i) => (i, ml)}
    actorId2ListOfMovies.setName("rdd_actor2ListOfMovies")

    // (actor, movie)
    val actorId2movie = actorId2ListOfMovies.flatMap(x => getActor2Movie(x))
    actorId2movie.setName("rdd_actor2movie")

    // (movie, actor)
    val movie2actorId = actorId2movie.map{case(a, m) => (m, a)}
    movie2actorId.setName("rdd_movie2actor")

    // (movie, (actor, actor))
    val movie2actorIdActorId = movie2actorId.join(movie2actorId)
    movie2actorIdActorId.setName("rdd_movie2ActorActor")

    // (actor, actor)
    val actorId2actorId = movie2actorIdActorId.map{case(m, (a1, a2)) => (a1, a2)}.filter(x => x._1 != x._2)
    actorId2actorId.setName("rdd_actor2actor")


    /////////////////////////////////////////////////////////////////////////////////////////////////


    // (id, name)
    val actors: RDD[(VertexId, String)] = actor2ListOfMoviesAndId.map{case((a, m), i) => (i, a)}

    // (edge[num]) => (idA, 0, idB)
    // edges has to have data, but we don't need it
    val collaborations: RDD[Edge[Int]] = actorId2actorId.map(x => Edge(x._1, x._2, 0))

    // graph => vertices(actor, text)
    val graph = Graph(actors, collaborations, "Missing")

    // graph => vertices(actor, shortestpath)
    val spResult = ShortestPaths.run(graph, Seq(KevinBaconID))

    // (id, dist)
    val baconNumbers = spResult.vertices
      .map{case(id, spmap) => (id, spmap.getOrElse(KevinBaconID, 100))}
      .filter(x => x._2 <= 6 && x._2 > 0)
    baconNumbers.setName("rdd_baconNumbers")


    /////////////////////////////////////////////////////////////////////////////////////////////////


    // (actor_id, actor)
    val actorID2actor = actor2ListOfMoviesAndId.map{case((actor, ml), id) => (id, actor)}

    // (actor, (gender, dist))
    val allActors = baconNumbers.join(actorID2actor).map{x =>
      val gender = if (x._1 >= 0) true else false
      (x._2._2, (gender, x._2._1))
    }.sortByKey()
    allActors.setName("rdd_allActors")

    val allActorsM = allActors.filter(x => x._2._1).map{case(n, (g, d)) => (n, d)}.collect()
    val allActorsF = allActors.filter(x => !x._2._1).map{case(n, (g, d)) => (n, d)}.collect()


    /////////////////////////////////////////////////////////////////////////////////////////////////


    println( "Writing results to actors.txt" )
    val fout = new PrintWriter(new File("output/actors.txt" ))

    ////

    val numAllActorsM = actor2ListOfMoviesAndId.filter(x => x._2 >= 0).count
    val numAllActorsF = actor2ListOfMoviesAndId.filter(x => x._2 < 0).count
    val numAllActors = numAllActorsM + numAllActorsF
    val numMovies = movie2actorId.groupByKey().count()

    val percAllActorsM = (numAllActorsM * 100) / numAllActors
    val percAllActorsF = (numAllActorsF * 100) / numAllActors

    fout.write("Total number of actors = " + numAllActors + ", out of which " + numAllActorsM + " (" + percAllActorsM + "%) are males while " + numAllActorsF + " (" + percAllActorsF + "%) are females.\n")
    fout.write("Total number of movies = " + numMovies + "\n\n")

    ////

		for (dist <- 1 until Distance + 1) {
      val countDistActorsM = allActorsM.count(x => x._2 == dist)
      val countDistActorsF = allActorsF.count(x => x._2 == dist)

			val percDistActorsM = countDistActorsM * 100.0 / numAllActorsM
			val percDistActorsF = countDistActorsF * 100.0 / numAllActorsF

			fout.write("There are " + countDistActorsM + " (" +  f"$percDistActorsM%.1f" + "%) actors and " + countDistActorsF + " (" + f"$percDistActorsF%.1f" + "%) actresses at distance " + dist + "\n")
		}

    ////

    val numActorsM = allActorsM.length
    val numActorsF = allActorsF.length
    val numActors = numActorsM + numActorsF

		fout.write("\nTotal number of actors from distance 1 to 6 = " + (numActorsM + numActorsF) + ", ratio = " + (numActors.toFloat / numAllActors) + "\n")
		fout.write("Total number of male actors from distance 1 to 6 = " + numActorsM + ", ratio = " + (numActorsM.toFloat / numAllActorsM) + "\n" )
		fout.write("Total number of female actors (actresses) from distance 1 to 6 = " + numActorsF + ", ratio = " + (numActorsF.toFloat / numAllActorsF) + "\n")

    ////

		fout.write( "\nList of male actors at distance 6:\n" )
    allActorsM
      .filter(x => x._2 == 6)
      .zipWithIndex.foreach(x => {
        fout.write(x._2 + ". " + x._1._1 + "\n")
      })

    fout.write( "\nList of female actors (actresses) at distance 6:\n" )
    allActorsF
      .filter(x => x._2 == 6)
      .zipWithIndex.foreach(x => {
        fout.write(x._2 + ". " + x._1._1 + "\n")
      })


    /////////////////////////////////////////////////////////////////////////////////////////////////


    fout.close()
    sc.stop()
    bw.close()

		val et = (System.currentTimeMillis - t0) / 1000
		val mins = et / 60
		val secs = et % 60
		println( "{Time taken = %d mins %d secs}".format(mins, secs) )
	}
}