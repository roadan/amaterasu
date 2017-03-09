package io.shinto.amaterasu.executor.mesos.executors

import java.io.{ByteArrayInputStream, ByteArrayOutputStream }

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.common.dataobjects.{ExecData, TaskData}
import io.shinto.amaterasu.common.logging.Logging
import org.apache.mesos.Protos._
import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver}
import org.apache.spark.SparkContext

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  var master: String = _
  var executorDriver: ExecutorDriver = _
  var sc: SparkContext = _
  var jobId: String = _
  var actionName: String = _
  var notifier: MesosNotifier = _
  var providersFactory: ProvidersFactory = _

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)


  override def shutdown(driver: ExecutorDriver): Unit = {

  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID) = ???

  override def disconnected(driver: ExecutorDriver) = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    this.executorDriver = driver
  }

  override def error(driver: ExecutorDriver, message: String): Unit = {

    val status = TaskStatus.newBuilder
      .setData(ByteString.copyFromUtf8(message))
      .setState(TaskState.TASK_ERROR).build()

    driver.sendStatusUpdate(status)

  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {

    this.executorDriver = driver
    val data = mapper.readValue(new ByteArrayInputStream(executorInfo.getData.toByteArray), classOf[ExecData])

    notifier = new MesosNotifier(driver)
    notifier.info(s"Executor ${executorInfo.getExecutorId.getValue} registered")

    val outStream = new ByteArrayOutputStream()
    providersFactory = ProvidersFactory(data, jobId, outStream, notifier, executorInfo.getExecutorId.getValue)

  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {

    notifier.info(s"launching task: ${taskInfo.getTaskId.getValue}")
    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_STARTING).build()

    driver.sendStatusUpdate(status)

    val task = Future {

      val taskData = mapper.readValue(new ByteArrayInputStream(taskInfo.getData.toByteArray), classOf[TaskData])

      val status = TaskStatus.newBuilder
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_RUNNING).build()

      driver.sendStatusUpdate(status)

      val runner = providersFactory.getRunner(taskData.groupId, taskData.typeId)
      runner match {
        case Some(r) => r.executeSource(taskData.src, actionName)
        case None =>
          notifier.error("", s"Runner not found for group: ${taskData.groupId}, type ${taskData.typeId}. Please verify the tasks")
          None
      }

    }

    task onComplete {
      case Failure(t) =>
        println(s"launching task failed: ${t.getMessage}")
        System.exit(1)
      case Success(ts) =>
        driver.sendStatusUpdate(TaskStatus.newBuilder
          .setTaskId(taskInfo.getTaskId)
          .setState(TaskState.TASK_FINISHED).build)
        notifier.info(s"complete task: ${taskInfo.getTaskId.getValue}")

    }

  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new ActionsExecutor
    executor.jobId = args(0)
    executor.master = args(1)
    executor.actionName = args(2)

    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}