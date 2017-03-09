package io.shinto.amaterasu.leader.execution.actions

import java.util.concurrent.BlockingQueue

import io.shinto.amaterasu.common.dataobjects.ActionData
import io.shinto.amaterasu.enums.ActionStatus
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection.mutable.ListBuffer

class SequentialAction extends Action {

  var jobId: String = _
  var jobsQueue: BlockingQueue[ActionData] = _
  var attempts: Int = 2
  var attempt: Int = 1

  def execute(): Unit = {

    try {

      announceQueued
      jobsQueue.add(data)

    }
    catch {

      //TODO: this will not invoke the error action
      case e: Exception => handleFailure(e.getMessage)

    }

  }

  override def handleFailure(message: String): String = {

    log.debug(s"Part ${data.name} of group ${data.groupId} and of type ${data.typeId} failed on attempt $attempt with message: $message")
    attempt += 1

    if (attempt <= attempts) {
      data.id
    }
    else {
      announceFailure()
      println(s"===> moving to err action ${data.errorActionId}")
      data.status = ActionStatus.failed
      data.errorActionId
    }

  }

}

object SequentialAction {

  def apply(name: String,
            src: String,
            groupId: String,
            typeId: String,
            jobId: String,
            queue: BlockingQueue[ActionData],
            zkClient: CuratorFramework,
            attempts: Int,
            exports: Map[String, String]): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"/$jobId/task-", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf("task-") + 5)

    action.attempts = attempts
    action.jobId = jobId
    action.data = ActionData(ActionStatus.pending, name, src, groupId, typeId, action.actionId, new ListBuffer[String], exports)
    action.jobsQueue = queue
    action.client = zkClient

    action
  }

}

object ErrorAction {

  def apply(name: String,
            src: String,
            parent: String,
            groupId: String,
            typeId: String,
            jobId: String,
            queue: BlockingQueue[ActionData],
            zkClient: CuratorFramework): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/task-$parent-error", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf('-') + 1).replace("/", "-")

    action.jobId = jobId
    action.data = ActionData(ActionStatus.pending, name, src, groupId, typeId, action.actionId, new ListBuffer[String], Map())
    action.jobsQueue = queue
    action.client = zkClient

    action

  }
}