package io.shinto.amaterasu.common.dataobjects

import io.shinto.amaterasu.enums.ActionStatus.ActionStatus

import scala.collection.mutable.ListBuffer

case class ActionData(var status: ActionStatus,
                      name: String,
                      src: String,
                      groupId: String,
                      typeId: String,
                      id: String,
                      nextActionIds: ListBuffer[String],
                      exports: Map[String, String]) {
  var errorActionId: String = null
}