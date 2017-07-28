/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.repl

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import io.netty.channel.ChannelHandlerContext
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

import org.apache.livy.Logging
import org.apache.livy.rsc.{BaseProtocol, ReplJobResults, RSCConf}
import org.apache.livy.rsc.BaseProtocol.ReplState
import org.apache.livy.rsc.driver._
import org.apache.livy.rsc.rpc.Rpc
import org.apache.livy.sessions._

class ReplDriver(conf: SparkConf, livyConf: RSCConf)
  extends RSCDriver(conf, livyConf)
  with Logging {

  private[repl] var session: Session = _

  private val kind = Kind(livyConf.get(RSCConf.Entry.SESSION_KIND))

  private[repl] val interpGroup = new InterpreterGroup

  override protected def initializeRepl(): Unit = {
    require(this.jobContext() != null, "JobContextImpl is not initialized")

    kind match {
      case PySpark() =>
        interpGroup.register(PythonInterpreter(conf, PySpark(), this.jobContext()))
      case PySpark3() =>
        interpGroup.register(PythonInterpreter(conf, PySpark3(), this.jobContext()))
      case Spark() =>
        interpGroup.register(new SparkInterpreter(conf))
      case SparkR() =>
        interpGroup.register(SparkRInterpreter(conf))
      case Shared() =>
        Seq(PythonInterpreter(conf, PySpark()),
          new SparkInterpreter(conf),
          SparkRInterpreter(conf)).foreach { i => interpGroup.register(i) }
    }

    session = new Session(livyConf,
      interpGroup,
      this.jobContext(),
      { s => broadcast(new ReplState(s.toString)) })

    Option(Await.result(session.start(), Duration.Inf))
      .map(new JavaSparkContext(_))
      .orNull
  }

  override protected def shutdownContext(): Unit = {
    if (session != null) {
      try {
        session.close()
      } finally {
        super.shutdownContext()
      }
    }
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.ReplJobRequest): Int = {
    session.execute(msg.code)
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.CancelReplJobRequest): Unit = {
    session.cancel(msg.id)
  }

  /**
   * Return statement results. Results are sorted by statement id.
   */
  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.GetReplJobResults): ReplJobResults = {
    val statements = if (msg.allResults) {
      session.statements.values.toArray
    } else {
      assert(msg.from != null)
      assert(msg.size != null)
      if (msg.size == 1) {
        session.statements.get(msg.from).toArray
      } else {
        val until = msg.from + msg.size
        session.statements.filterKeys(id => id >= msg.from && id < until).values.toArray
      }
    }

    // Update progress of statements when queried
    statements.foreach { s =>
      s.updateProgress(session.progressOfStatement(s.id))
    }

    new ReplJobResults(statements.sortBy(_.id))
  }

  override protected def createWrapper(msg: BaseProtocol.BypassJobRequest): BypassJobWrapper = {
    Kind(msg.jobTpe) match {
      case PySpark() | PySpark3() => new BypassJobWrapper(this, msg.id,
        new BypassPySparkJob(msg.serializedJob, this))
      case _ => super.createWrapper(msg)
    }
  }

  override protected def addFile(path: String): Unit = {
    require(!interpGroup.isEmpty())
    interpGroup.allInterpreters().filter(_._2.isInstanceOf[PythonInterpreter]).foreach { i =>
      i.asInstanceOf[PythonInterpreter].addFile(path)
    }
    super.addFile(path)
  }

  override protected def addJarOrPyFile(path: String): Unit = {
    require(!interpGroup.isEmpty())
    interpGroup.allInterpreters().filter(_._2.isInstanceOf[PythonInterpreter]).foreach { i =>
      i.asInstanceOf[PythonInterpreter].addPyFile(this, conf, path)
    }
    super.addJarOrPyFile(path)
  }

  override protected def onClientAuthenticated(client: Rpc): Unit = {
    if (session != null) {
      client.call(new ReplState(session.state.toString))
    }
  }
}
