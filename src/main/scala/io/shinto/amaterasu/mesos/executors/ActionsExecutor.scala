package io.shinto.amaterasu.mesos.executors

import com.google.protobuf.ByteString
import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.{ ClusterConfig, SparkConfig }
import org.apache.spark.repl.runners.spark.SparkScalaRunner
import org.apache.mesos.Protos._
import org.apache.mesos.{ MesosExecutorDriver, ExecutorDriver, Executor }
import org.apache.spark.repl.Main
import org.apache.spark.{ SparkConf, SparkContext }

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  var executorDriver: ExecutorDriver = null
  var sc: SparkContext = null
  var jobId: String = null

  override def shutdown(driver: ExecutorDriver): Unit = ???

  override def disconnected(driver: ExecutorDriver): Unit = ???

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    this.executorDriver = driver
  }

  override def error(driver: ExecutorDriver, message: String): Unit = {

    val status = TaskStatus.newBuilder
      .setData(ByteString.copyFromUtf8(message))
      .setState(TaskState.TASK_ERROR).build()

    driver.sendStatusUpdate(status)

  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
    this.executorDriver = driver
  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {

    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_RUNNING).build()

    driver.sendStatusUpdate(status)
    val actionSource = taskInfo.getData().toStringUtf8()

    val actionName = "action-" + taskInfo.getTaskId.getValue
    val sparkAppName = s"job_${jobId}_executor_${taskInfo.getExecutor.getExecutorId.getValue}"

    try {
      val env = Environment()
      //env.workingDir = "s3n://AKIAJRS2MF7ZFKYB4G3A:wkYExzDuLLkGQYGvOA0slbOHzF38PVI9DyT+tSph@amaterasu/work_new/"
      env.workingDir = "file:///tmp/worky/"
      env.master = "mesos://192.168.33.11:5050"

      //      if (sc == null)
      //        sc = createSparkContext(env, sparkAppName)

      //sc.getConf.getAll
      val sparkScalaRunner = SparkScalaRunner(env, jobId, sc)
      sparkScalaRunner.executeSource(actionSource, actionName)
      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_FINISHED).build())
    }
    catch {
      case e: Exception => {
        println(s"launching task failed: ${e.getMessage}")

        System.exit(1)
      }
    }
  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new ActionsExecutor
    executor.jobId = args(0)

    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}