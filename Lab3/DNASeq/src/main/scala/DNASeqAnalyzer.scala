import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

import htsjdk.samtools._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.{SparkConf, SparkContext}
import tudelft.utils.{ChromosomeRange, Configuration, SAMRecordIterator}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.sys.process._

object DNASeqAnalyzer {
  final val MemString = "-Xmx2048m"
  final val RefFileName = "ucsc.hg19.fasta"
  final val SnpFileName = "dbsnp_138.hg19.vcf"
  final val ExomeFileName = "gcat_set_025.bed"
  val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkListener.txt"), "UTF-8"))


  //////////////////////////////////////////////////////////////////////////////

  // Read the SAM files generated from part 2
  def bwaRead(x: String): Array[(Int, SAMRecord)] = {

    val bwaKeyValues = new BWAKeyValues(x)
    bwaKeyValues.parseSam()
    val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

    kvPairs
  }

  // Deprecated
  def bwaRun(x: String, bcconfig: Broadcast[Configuration]): Array[(Int, SAMRecord)] = {
    val config = bcconfig.value
    val refFolder = config.getRefFolder
    val toolsFolder = config.getToolsFolder
    val numThreads = config.getNumThreads
    val tmpFolder = config.getTmpFolder

    val inputFile = x
    val outFileName = tmpFolder + "bwamem" + x.filter(_.isDigit)
    println(inputFile)
    println(outFileName)

    val command = Seq(toolsFolder + "bwa", "mem", refFolder + RefFileName, "-p", "-t", numThreads, inputFile)
    println(command)
    (command #> new File(outFileName)).!

    val bwaKeyValues = new BWAKeyValues(outFileName)
    bwaKeyValues.parseSam()
    val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

    //new File(outFileName).delete
    Seq("rm", outFileName).!

    kvPairs
  }

  def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], bcconfig: Broadcast[Configuration]): ChromosomeRange = {
    val config = bcconfig.value
    val header = new SAMFileHeader()
    header.setSequenceDictionary(config.getDict)
    val outHeader = header.clone()
    outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    val factory = new SAMFileWriterFactory()
    val writer = factory.makeBAMWriter(outHeader, true, new File(fileName))

    val r = new ChromosomeRange()
    val input = new SAMRecordIterator(samRecordsSorted, header, r)
    while (input.hasNext) {
    val sam = input.next()
    writer.addAlignment(sam)
    }
    writer.close()

    r
  }

  def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = {
    if(a.getReferenceIndex == b.getReferenceIndex)
      a.getAlignmentStart - b.getAlignmentStart
    else
      a.getReferenceIndex - b.getReferenceIndex
  }


  //////////////////////////////////////////////////////////////////////////////


  def variantCall(chrRegion: Int, samRecords: Array[SAMRecord], bcconfig: Broadcast[Configuration]): Array[(Int, (Int, String))] = {
    val config = bcconfig.value
    val tmpFolder = config.getTmpFolder
    val toolsFolder = config.getToolsFolder
    val refFolder = config.getRefFolder
    val numOfThreads = config.getNumThreads

    // Following is shown how each tool is called. Replace the X in regionX with the chromosome region number (chrRegion).
    // 	You would have to create the command strings (for running jar files) and then execute them using the Scala's process package. More
    // 	help about Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.
    //	Note that MemString here is -Xmx14336m, and already defined as a constant variable above, and so are reference files' names.

    /*************************************/

    val samRecordsSorted = samRecords.sortWith{case(first, second) => compareSAMRecords(first, second) < 0}

    val p1 = tmpFolder + s"/region$chrRegion-p1.bam"
    val p2 = tmpFolder + s"/region$chrRegion-p2.bam"
    val p3 = tmpFolder + s"/region$chrRegion-p3.bam"
    val p3_metrics = tmpFolder + s"/region$chrRegion-p3-metrics.txt"
    val regionFile = tmpFolder + s"/region$chrRegion.bam"

    // SAM records should be sorted by this point
    val chrRange = writeToBAM(p1, samRecordsSorted, bcconfig)

    /*************************************/

    // Picard preprocessing
    //	java MemString -jar toolsFolder/CleanSam.jar INPUT=tmpFolder/regionX-p1.bam OUTPUT=tmpFolder/regionX-p2.bam
    var command = Seq("java", MemString, "-jar", toolsFolder + "CleanSam.jar", "INPUT=" + p1, "OUTPUT=" + p2)
    println(command)
    command.!

    //	java MemString -jar toolsFolder/MarkDuplicates.jar INPUT=tmpFolder/regionX-p2.bam OUTPUT=tmpFolder/regionX-p3.bam
    //		METRICS_FILE=tmpFolder/regionX-p3-metrics.txt
    command = Seq("java", MemString, "-jar", toolsFolder + "MarkDuplicates.jar", "INPUT=" + p2, "OUTPUT=" + p3, "METRICS_FILE=" + p3_metrics)
    println(command)
    command.!

    //	java MemString -jar toolsFolder/AddOrReplaceReadGroups.jar INPUT=tmpFolder/regionX-p3.bam OUTPUT=tmpFolder/regionX.bam
    //		RGID=GROUP1 RGLB=LIB1 RGPL=ILLUMINA RGPU=UNIT1 RGSM=SAMPLE1
    command = Seq("java", MemString, "-jar", toolsFolder + "AddOrReplaceReadGroups.jar", "INPUT=" + p3, "OUTPUT=" + regionFile, "RGID=GROUP1", "RGLB=LIB1", "RGPL=ILLUMINA", "RGPU=UNIT1", "RGSM=SAMPLE1")
    println(command)
    command.!

    // 	java MemString -jar toolsFolder/BuildBamIndex.jar INPUT=tmpFolder/regionX.bam
    command = Seq("java", MemString, "-jar", toolsFolder + "BuildBamIndex.jar", "INPUT=" + regionFile)
    println(command)
    command.!

    //	delete tmpFolder/regionX-p1.bam, tmpFolder/regionX-p2.bam, tmpFolder/regionX-p3.bam and tmpFolder/regionX-p3-metrics.txt
    Seq("rm", p1, p2, p3, p3_metrics).!

    /*************************************/

    // Make region file
    val tmpBedFile = tmpFolder + s"tmp$chrRegion.bed"
    val bedFile = tmpFolder + s"bed$chrRegion.bed"

    //	val tmpBed = new File(tmpFolder/tmpX.bed)
    val tmpBed = new File(tmpBedFile)

    //	chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())
    chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath)

    //	toolsFolder/bedtools intersect -a refFolder/ExomeFileName -b tmpFolder/tmpX.bed -header > tmpFolder/bedX.bed
    (Seq(toolsFolder + "bedtools", "intersect", "-a", refFolder + ExomeFileName, "-b", tmpBedFile, "-header") #> new File(bedFile)).!

    //	delete tmpFolder/tmpX.bed
    Seq("rm", tmpBedFile).!

    /*************************************/

    // Indel Realignment
    val intervalFile = tmpFolder + s"region$chrRegion.intervals"
    val region2File = tmpFolder + s"region$chrRegion-2.bam"
    val baiFile = tmpFolder + s"region$chrRegion.bai"

    //	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T RealignerTargetCreator -nt numOfThreads -R refFolder/RefFileName
    //		-I tmpFolder/regionX.bam -o tmpFolder/regionX.intervals -L tmpFolder/bedX.bed
    command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "RealignerTargetCreator", "-nt", numOfThreads, "-R", refFolder + RefFileName, "-I", regionFile, "-o", intervalFile, "-L", bedFile)
    println(command)
    command.!

    //	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T IndelRealigner -R refFolder/RefFileName -I tmpFolder/regionX.bam
    //		-targetIntervals tmpFolder/regionX.intervals -o tmpFolder/regionX-2.bam -L tmpFolder/bedX.bed
    command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "IndelRealigner", "-R", refFolder + RefFileName, "-I", regionFile, "-targetIntervals", intervalFile, "-o", region2File, "-L", bedFile)
    println(command)
    command.!

    //	delete tmpFolder/regionX.bam, tmpFolder/regionX.bai, tmpFolder/regionX.intervals
    Seq("rm", intervalFile, baiFile).! //, intervalFile).!

    /*************************************/

    // Base quality recalibration
    val regionTableFile = tmpFolder + s"region$chrRegion.table"
    val region3File = tmpFolder + s"region$chrRegion-3.bam"
    val bai2File = tmpFolder + s"region$chrRegion-2.bai"

    //	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T BaseRecalibrator -nct numOfThreads -R refFolder/RefFileName -I
    //		tmpFolder/regionX-2.bam -o tmpFolder/regionX.table -L tmpFolder/bedX.bed --disable_auto_index_creation_and_locking_when_reading_rods
    //		-knownSites refFolder/SnpFileName
    command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "BaseRecalibrator", "-nct", numOfThreads, "-R", refFolder + RefFileName, "-I", region2File, "-o", regionTableFile, "-L", bedFile, "--disable_auto_index_creation_and_locking_when_reading_rods", "-knownSites", refFolder + SnpFileName)
    println(command)
    command.!

    //	java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T PrintReads -R refFolder/RefFileName -I
    //		tmpFolder/regionX-2.bam -o tmpFolder/regionX-3.bam -BSQR tmpFolder/regionX.table -L tmpFolder/bedX.bed
    command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "PrintReads", "-R", refFolder + RefFileName, "-I", region2File, "-o", region3File, "-BQSR", regionTableFile, "-L", bedFile)
    println(command)
    command.!

    // delete tmpFolder/regionX-2.bam, tmpFolder/regionX-2.bai, tmpFolder/regionX.table
    Seq("rm", region2File, bai2File, regionTableFile).!

    /*************************************/

    // Haplotype -> Uses the region bed file
    val vcfFile = tmpFolder + s"region$chrRegion.vcf"
    val bai3File = tmpFolder + s"region$chrRegion-3.bai"

    // java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T HaplotypeCaller -nct numOfThreads -R refFolder/RefFileName -I
    //		tmpFolder/regionX-3.bam -o tmpFolder/regionX.vcf  -stand_call_conf 30.0 -stand_emit_conf 30.0 -L tmpFolder/bedX.bed
    //		--no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods
    command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "HaplotypeCaller", "-nct", numOfThreads, "-R", refFolder + RefFileName, "-I", region3File, "-o", vcfFile, "-stand_call_conf", "30.0", "-stand_emit_conf", "30.0", "-L", bedFile, "--no_cmdline_in_header", "--disable_auto_index_creation_and_locking_when_reading_rods")
    println(command)
    command.!

    // delete tmpFolder/regionX-3.bam, tmpFolder/regionX-3.bai, tmpFolder/bedX.bed
    command = Seq("rm", region3File, bai3File, bedFile)
    println(command)
    command.!

    /*************************************/

    var results = ArrayBuffer[(Int, (Int, String))]()
    val resultFile = Source.fromFile(vcfFile)

    resultFile.getLines().foreach(line => {
      if (!line.startsWith("#")) {
        val tabs = line.split("\t")
        var chrom = 0

        if (tabs(0) == "chrX") {
          chrom = 23
        } else {
          chrom = tabs(0).filter(_.isDigit).toInt
        }

        val pos = tabs(1).toInt
        results += ((chrom, (pos, line)))
      }
    })

    println("steady")
    results.toArray
  }


  //////////////////////////////////////////////////////////////////////////////


  def loadBalancer(weights: Array[(Int, Int)], numTasks: Int): ArrayBuffer[ArrayBuffer[Int]] = {
    val results = ArrayBuffer.fill(numTasks)(ArrayBuffer[Int]())
    val sizes = ArrayBuffer.fill(numTasks)(0)

    weights
      .sorted
      .reverse
      .foreach{case(distance, key) =>
        val region = sizes.zipWithIndex.min._2
        sizes(region) += distance
        results(region) += key
      }

    results
  }

  def getTimeStamp: String = {
    "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime) + "] "
  }


  //////////////////////////////////////////////////////////////////////////////


  def main(args: Array[String]) {
    val config = new Configuration()
    config.initialize()

    val numInstances = Integer.parseInt(config.getNumInstances)
    val inputFolder = config.getInputFolder
    val outputFolder = config.getOutputFolder

    /*************************************/

    val mode = "local"
    val conf = new SparkConf().setAppName("DNASeqAnalyzer")

    // For local mode, include the following two lines
    if (mode == "local") {
      conf.setMaster("local[" + config.getNumInstances + "]")
      conf.set("spark.cores.max", config.getNumInstances)
    }
    if (mode == "cluster") {
      // For cluster mode, include the following commented line
      conf.set("spark.shuffle.blockTransferService", "nio")
    }
    //conf.set("spark.rdd.compress", "true")

    /*************************************/

    new File(outputFolder).mkdirs
    new File(outputFolder + "output.vcf")
    val sc = new SparkContext(conf)
    val bcconfig = sc.broadcast(config)

    // Comment these two lines if you want to see more verbose messages from Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /*************************************/

    sc.addSparkListener(new SparkListener() {
      override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
        bw.write(getTimeStamp + " Spark ApplicationStart: " + applicationStart.appName + "\n")
        bw.flush
      }

      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
        bw.write(getTimeStamp + " Spark ApplicationEnd: " + applicationEnd.time + "\n")
        bw.flush
      }

      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
        val map = stageCompleted.stageInfo.rddInfos
        map.foreach(row => {
          if (row.isCached) {
            bw.write(getTimeStamp + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " +
              row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n")
          }
          else if (row.name.contains("rdd_")) {
            bw.write(getTimeStamp + row.name + " processed!\n")
          }
          bw.flush
        })
      }
    })

    /*************************************/

    val t0 = System.currentTimeMillis

    val files = sc.parallelize(new File(inputFolder).listFiles, numInstances)
    files.cache

    println("inputFolder = " + inputFolder + ", list of files = ")
    files.collect.foreach(x => println(x))

    /** ***********************************/

    val bwaResults = files.flatMap(files => bwaRead(files.getPath))
      .combineByKey(
        (sam: SAMRecord) => Array(sam),
        (acc: Array[SAMRecord], value: SAMRecord) => acc :+ value,
        (acc1: Array[SAMRecord], acc2: Array[SAMRecord]) => acc1 ++ acc2
      ).persist(MEMORY_ONLY_SER) //cache
    bwaResults.setName("rdd_bwaResults")

    val loadPerChromosome = bwaResults.map { case (key, values) => (values.length, key) }.collect
    val loadMap = loadBalancer(loadPerChromosome, numInstances)

    val loadBalancedRdd = bwaResults.map{ case(key, values) =>
      (loadMap.indexWhere((a: ArrayBuffer[Int]) => a.contains(key)), values)
    }.reduceByKey(_ ++ _)
    loadBalancedRdd.setName("rdd_loadBalancedRdd")

    val variantCallData = loadBalancedRdd
      .flatMap { case(key, sams) =>
        variantCall(key, sams, bcconfig)
      }
    variantCallData.setName("rdd_variantCallData")

    val results = variantCallData.combineByKey(
      (value: (Int, String)) => Array(value),
      (acc: Array[(Int, String)], value: (Int, String)) => acc :+ value,
      (acc1: Array[(Int, String)], acc2: Array[(Int, String)]) => acc1 ++ acc2
    ).cache
    results.setName("rdd_results")

    /*************************************/

    val fl = new PrintWriter(new File(outputFolder + "output.vcf"))
    (1 to 24).foreach(i => {
      println("Writing chrom: " + i.toString)

      val fileDump = results
        .filter{case(chrom, value) => chrom == i}
        .flatMap{case(chrom, value) => value}
        .sortByKey(ascending = true)
        .map{case(position, line) => line}
        .collect

      fileDump.toIterator.foreach(line => fl.println(line))
    })

    /*************************************/

    fl.close()
    sc.stop()
    bw.close()

    val et = (System.currentTimeMillis - t0) / 1000
    println(getTimeStamp + "Execution time: %d mins %d secs".format(et/60, et%60))
  }
}