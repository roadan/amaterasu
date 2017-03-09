package io.shinto.amaterasu.executor.execution.actions.runners.spark

import java.io.{ByteArrayOutputStream, File}

import io.shinto.amaterasu.common.dataobjects.ExecData
import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.execution.dependencies.Dependencies
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import org.apache.spark.repl.amaterasu.ReplUtils
import org.eclipse.aether.util.artifact.JavaScopes
import com.jcabi.aether.Aether
import io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
  * Created by roadan on 2/9/17.
  */
class SparkRunnersProvider extends RunnersProvider with Logging {

  private val runners = new TrieMap[String, AmaterasuRunner]

  override def init(data: ExecData, jobId: String, outStream: ByteArrayOutputStream, notifier: Notifier, executorId: String): Unit = {

    var jars = Seq[String]()
    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    val classServerUri = ReplUtils.getOrCreateClassServerUri(outStream, jars)

    val sparkAppName = s"job_${jobId}_executor_$executorId"
    //log.debug(s"creating SparkContext with master ${data.env.master}")
    val sparkContext = SparkRunnerHelper.createSparkContext(data.env, sparkAppName, classServerUri, jars)

    val sparkScalaRunner = SparkScalaRunner(data.env, jobId, sparkContext, outStream, notifier, jars, data.exports)
    sparkScalaRunner.initializeAmaContext(data.env)

    runners.put(sparkScalaRunner.getIdentifier, sparkScalaRunner)

    val pySparkRunner = PySparkRunner(data.env, jobId, notifier, sparkContext, "")
    runners.put(pySparkRunner.getIdentifier(), pySparkRunner)
  }

  override def getGroupIdentifier: String = "spark"

  override def getRunner(id: String): AmaterasuRunner = {

    runners(id)

  }

  private def getDependencies(deps: Dependencies): Seq[String] = {

    // adding a local repo because Aether needs one
    val repo = new File(System.getProperty("java.io.tmpdir"), "ama-repo")

    val remotes = deps.repos.map(r =>
      new RemoteRepository(
        r.id,
        r.`type`,
        r.url
      )).toList.asJava

    val aether = new Aether(remotes, repo)

    deps.artifacts.flatMap(a => {
      aether.resolve(
        new DefaultArtifact(a.groupId, a.artifactId, "", "jar", a.version),
        JavaScopes.RUNTIME
      ).map(a => a) // .toBuffer[Artifact]
    }).map(x => x.getFile.getAbsolutePath)

  }
}