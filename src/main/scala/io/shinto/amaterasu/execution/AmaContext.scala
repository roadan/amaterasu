package io.shinto.amaterasu.execution

import io.shinto.amaterasu.configuration.environments.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SaveMode, DataFrame, SQLContext }
import org.apache.spark.SparkContext

object AmaContext {

  var sc: SparkContext = null
  var jobId: String = null
  var sqlContext: SQLContext = null
  var env: Environment = null

  def init(
    sc: SparkContext,
    sqlContext: SQLContext,
    jobId: String,
    env: Environment
  ) = {

    AmaContext.sc = sc
    AmaContext.sqlContext = sqlContext
    AmaContext.jobId = jobId
    AmaContext.env = env

  }

  def saveDataFrame(df: DataFrame, actionName: String, dfName: String) = {

    println(s"${env.workingDir}/$jobId/$actionName/$dfName")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def saveRDD(rdd: RDD[_], actionName: String, rddName: String) = {

    rdd.saveAsObjectFile(s"${env.workingDir}/$jobId/$actionName/$rddName")

  }

  def getDataFrame(actionName: String, dfName: String): DataFrame = {

    AmaContext.sqlContext.read.parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def getRDD(actionName: String, rddName: String): RDD[_] = {

    AmaContext.sc.objectFile(s"${env.workingDir}/$jobId/$actionName/$rddName")

  }

  def getActionResult(actionName: String): DataFrame = {

    AmaContext.sqlContext.sql(s"select * from ${AmaContext.jobId}.$actionName")

  }

}
