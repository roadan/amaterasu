package org.apache.spark.repl.runners.spark

import java.io._
import java.lang.reflect.Method

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.execution.AmaContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.repl.{ SparkIMain, SparkCommandLine, SparkILoop }

import scala.{ Predef, StringBuilder }
import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ Results, IMain }

class AmaSparkILoop(writer: PrintWriter) extends SparkILoop(null, writer) {

  def create() = {
    this.createInterpreter
  }

}

class ResHolder(var value: Any)

class SparkScalaRunner extends Logging {

  // This is the amaterasu spark configuration need to rethink the name
  var env: Environment = null
  var jobId: String = null
  var interpreter: SparkIMain = null
  var out: PrintWriter = null
  var outStream: ByteArrayOutputStream = null
  var sc: SparkContext = null
  var classServerUri: String = null

  val settings = new Settings()
  val holder = new ResHolder(null)

  def execute(file: String, actionName: String): Unit = {
    initializeAmaContext(env)
    val source = Source.fromFile(file)
    interpretSources(source, actionName)
    interpreter.close()
  }

  def executeSource(actionSource: String, actionName: String): Unit = {
    println("~~~~~~~~~~~~~~~~~~~")
    initializeAmaContext(env)
    println("~~~~~~~~~~~~~~~~~~~")
    val source = Source.fromString(actionSource)
    interpretSources(source, actionName)
    interpreter.close()
  }

  def createSparkContext(env: Environment, jobId: String): SparkContext = {

    System.setProperty("hadoop.home.dir", "/home/hadoop/hadoop")

    val conf = new SparkConf(true)
      .setMaster(env.master)
      .setAppName(jobId)
      .set("spark.executor.uri", "http://192.168.33.11:8000/spark-1.6.1-1.tgz")
      .set("spark.io.compression.codec", "lzf")
      .set("spark.mesos.coarse", "false")
      .set("spark.mesos.mesosExecutor.cores", "1")
      .set("spark.hadoop.validateOutputSpecs", "false")
    // .set("hadoop.home.dir", "/home/hadoop/hadoop")
    //      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
    new SparkContext(conf)

  }

  def createSparkContextString(env: Environment, jobId: String): String = {

    val builder = new StringBuilder()

    builder.append("""System.setProperty("hadoop.home.dir", "/home/hadoop/hadoop")""")
    builder.append("\n")
    builder.append("val conf = new SparkConf(true)")
    builder.append(s""".setMaster("${env.master}")""")
    builder.append(s""".setAppName("$jobId")""")
    builder.append(s""".set("spark.repl.class.uri", "$classServerUri")""")
    builder.append(s""".set("spark.executor.uri", "http://192.168.33.11:8000/spark-1.6.1-1.tgz")""")
    builder.append(s""".set("spark.mesos.executor.home", "/home/vagrant/spark-1.6.1-1")""")
    builder.append(s""".set("spark.io.compression.codec", "lzf")""")
    builder.append(s""".set("spark.submit.deployMode","client")""")
    builder.append(s""".set("spark.mesos.coarse", "false")""")
    builder.append(s""".set("spark.mesos.mesosExecutor.cores", "1")""")
    builder.append(s""".set("spark.hadoop.validateOutputSpecs", "false")""")
    builder.append("\n")
    // .set("hadoop.home.dir", "/home/hadoop/hadoop")
    //      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
    builder.append("val sc = new SparkContext(conf)\n")
    builder.append("val sqlContext = new SQLContext(sc)\n")

    builder.toString
  }

  def interpretSources(source: Source, actionName: String): Unit = {
    for (line <- source.getLines()) {

      if (!line.isEmpty) {

        outStream.reset()
        log.debug(line)

        val intresult = interpreter.interpret(line)

        //if (interpreter.prevRequestList.last.value.exists) {

        val result = interpreter.prevRequestList.last.lineRep.call("$result")

        // dear future me (probably Karel or Tim) this is what we
        // can use
        // intresult: Success, Error, etc
        // result: the actual result (RDD, df, etc.) for caching
        // outStream.toString gives you the error message
        intresult match {
          case Results.Success => {
            log.debug("Results.Success")

            //val resultName = interpreter.prevRequestList.last.value.name.toString
            val resultName = interpreter.prevRequestList.last.termNames.last
            //println(interpreter.prevRequestList.last.value)
            if (result != null) {
              result match {
                case df: DataFrame => {
                  log.debug(s"persisting DataFrame: $resultName")
                  val x = interpreter.interpret(s"""$resultName.write.mode(SaveMode.Overwrite).parquet("${env.workingDir}/ama-$jobId/$actionName/$resultName")""")
                  log.debug(s"DF=> $x")
                  log.debug(outStream.toString)
                  //interpreter.interpret(s"""AmaContext.saveDataFrame($resultName, "$actionName", "$resultName")""")
                  log.debug(s"persisted DataFrame: $resultName")
                }
                case rdd: RDD[_] => {
                  log.debug(s"persisting RDD: $resultName")
                  val x = interpreter.interpret(s"""$resultName.saveAsObjectFile("${env.workingDir}/ama-$jobId/$actionName/$resultName")""")
                  log.debug(s"RDD=> $x")
                  log.debug(outStream.toString)
                  //interpreter.interpret(s"""AmaContext.saveRDD($resultName, "$actionName", "$resultName")""")
                  log.debug(s"persisted RDD: $resultName")
                }
                case _ => println(result)
              }
            }
          }
          case Results.Error => {
            log.debug("Results.Error")
            println(outStream.toString)
          }
          case Results.Incomplete => {
            log.debug("Results.Incomplete")
            log.debug("|")
          }
        }
        //}
      }
    }
  }

  def initializeAmaContext(env: Environment): Unit = {
    // setting up some context :)
    //    val sc = this.sc
    //      val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    try {
      interpreter.interpret("import scala.util.control.Exception._")
      interpreter.interpret("import org.apache.spark.{ SparkContext, SparkConf }")
      interpreter.interpret("import org.apache.spark.sql.SQLContext")
      interpreter.interpret("import io.shinto.amaterasu.execution.AmaContext")
      interpreter.interpret("import io.shinto.amaterasu.configuration.environments.Environment")

      val contextStrings = createSparkContextString(env, jobId)
      println(contextStrings)
      interpreter.interpret(contextStrings)

      //println(outStream.toString)

      // creating a map (_contextStore) to hold the different spark contexts
      // in th REPL and getting a reference to it
      interpreter.interpret("var _contextStore = scala.collection.mutable.Map[String, AnyRef]()")

      val contextStore = interpreter.prevRequestList.last.lineRep.call("$result").asInstanceOf[mutable.Map[String, AnyRef]]
      contextStore.put("env", env)
      interpreter.interpret("val env = _contextStore(\"env\").asInstanceOf[Environment]")
      interpreter.interpret(s"AmaContext.init(sc, sqlContext, $jobId, env)")

      interpreter.interpret("println(\"%%%%%%%%%%%%%%%%%\")")
      interpreter.interpret("val cl = ClassLoader.getSystemClassLoader")
      interpreter.interpret("cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(x => println(\"@@@\" + x))")
      interpreter.interpret("println(\"%%%%%%%%%%%%%%%%%\")")
      interpreter.interpret("println(sc.getConf.getAll.foreach(println))")
      interpreter.interpret("println(\"%%%%%%%%%%%%%%%%%\")")

      // populating the contextStore
      interpreter.interpret("""contextStore.put("sc", sc)""")
      interpreter.interpret("""contextStore.put("sqlContext", sqlContext)""")
      interpreter.interpret("""contextStore.put("ac", AmaContext)""")

      //interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
      //interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")

      interpreter.interpret("val AmaContext = _contextStore(\"ac\").asInstanceOf[AmaContext]")

      // initializing the AmaContext
      println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")
    }
    catch {
      case e: Exception => {
        println(s"exec error: ${e.getMessage}")
        println(e)
        throw e
      }
    }
  }

}

object SparkScalaRunner {

  def apply(env: Environment, jobId: String, sc: SparkContext): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.env = env
    result.jobId = jobId
    result.outStream = new ByteArrayOutputStream()
    val intp = creteInterprater(env, jobId, result.outStream)
    result.interpreter = intp._1
    result.classServerUri = intp._2
    //result.sc = sc
    result
  }

  def creteInterprater(env: Environment, jobId: String, outStream: ByteArrayOutputStream): (SparkIMain, String) = {

    var result: SparkIMain = null
    var classServerUri: String = null

    try {

      val command =
        new SparkCommandLine(List( //          "-Yrepl-class-based",
        //          "-Yrepl-outdir", s"./",
        //          "-classpath", System.getProperty("java.class.path") + ":" +
        //            "spark-assembly-1.6.1-hadoop2.4.0.jar:spark-core_2.10-1.6.1.jar"
        ))

      //TODO: revisit this, not sure it should be in an apply method
      val settings = command.settings

      settings.classpath.append(System.getProperty("java.class.path") + ":" +
        "spark-assembly-1.6.1-hadoop2.4.0.jar" + File.pathSeparator + "/home/hadoop/hadoop/etc/hadoop")
      println("{{{{{}}}}}")
      println(settings.classpath)

      settings.usejavacp.value = true

      println(":::::::::::::::::::::")
      val in: Option[BufferedReader] = null
      //val outStream = new ByteArrayOutputStream()
      val out = new PrintWriter(outStream)
      //result.interpreter
      val interpreter = new AmaSparkILoop(out)
      interpreter.settings = settings

      interpreter.create

      val intp = interpreter.intp

      try {
        val classServer: Method = intp.getClass.getMethod("classServerUri")
        classServerUri = classServer.invoke(intp).asInstanceOf[String]
      }
      catch {
        case e: Any => {
          println(String.format("Spark method classServerUri not available due to: [%s]", e.getMessage))
        }
      }

      println(classServerUri)
      println(":::::::::::::::::::::")

      settings.embeddedDefaults(Thread.currentThread()
        .getContextClassLoader())
      intp.setContextClassLoader
      println(":::::::::::::::::::::")
      intp.initializeSynchronous
      //intp.bind("$result", result.holder.getClass.getName, result.holder)

      result = intp
    }
    catch {
      case e: Exception => {
        println("+++++++>" + new Predef.String(outStream.toByteArray))
      }
    }
    println(":::::::::::::::::::::")
    (result, classServerUri)
  }
}
