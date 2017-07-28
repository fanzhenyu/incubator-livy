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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.livy.Logging
import org.apache.livy.repl.Interpreter.{ExecuteAborted, ExecuteResponse}

/**
 * A [[InterpreterGroup]] to maintain all the created interpreters in this session. Each
 * interpreter has a unique id to represent itself, [[InterpreterGroup]] supports dynamically
 * register new interpreter, or unregister existed one.
 */
private[repl] final class InterpreterGroup extends Logging {
  private val interpreterId = new AtomicInteger(0)

  // A hashmap to maintain all the registered interpreters.
  private val interpreters = new mutable.HashMap[Int, Interpreter]

  /**
   * Check if there's no interpreter registered.
   */
  def isEmpty(): Boolean = interpreters.synchronized {
    interpreters.isEmpty
  }
  /**
   * Get all the registered interpreters with related id.
   */
  def allInterpreters(): Map[Int, Interpreter] = interpreters.synchronized {
    interpreters.toMap
  }

  /**
   * Register interpreter to this group, return an id to represent this interpreter.
   */
  def register(interpreter: Interpreter): Int = interpreters.synchronized {
    val currentId = interpreterId.getAndIncrement()
    interpreters.put(currentId, interpreter)
    debug(s"Register interpreter ${interpreter.kind} with id $currentId")
    currentId
  }

  /**
   * Unregister interpreter with specified id.
   */
  def unregister(id: Int): Unit = interpreters.synchronized {
    interpreters.remove(id).foreach { i =>
      debug(s"Unregister interpreter ${i.kind} with id $id")
    }
  }

  /**
   * Submit code to specified interpreter to get executed, and return the result. If interpreter
   * with specified id cannot be found, then an error will be returned.
   */
  def execute(id: Int, code: String): ExecuteResponse = {
    val interpOpt = interpreters.synchronized {
      interpreters.get(id)
    }
    interpOpt match {
      case Some(i) => i.execute(code)
      case None => ExecuteAborted(s"Failed to find interpreter with id $id")
    }
  }

  /**
   * Unregister and close all the interpreters, this is usually called when session is closed.
   */
  def unregisterAndCloseAll(): Unit = interpreters.synchronized {
    interpreters.foreach(_._2.close())
    interpreters.clear()
  }
}
