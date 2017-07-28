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

package org.apache.livy.rsc.driver;

import java.io.File;
import java.lang.reflect.Method;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.JobContext;
import org.apache.livy.rsc.Utils;

class JobContextImpl implements JobContext {

  private static final Logger LOG = LoggerFactory.getLogger(JobContextImpl.class);

  private final JavaSparkContext sc;
  private final File localTmpDir;
  private volatile SQLContext sqlctx;
  private volatile HiveContext hivectx;
  private volatile JavaStreamingContext streamingctx;
  private final RSCDriver driver;
  private volatile Object sparksession;

  public JobContextImpl(JavaSparkContext sc, File localTmpDir, RSCDriver driver) {
    this.sc = sc;
    this.localTmpDir = localTmpDir;
    this.driver = driver;
  }

  @Override
  public JavaSparkContext sc() {
    return sc;
  }

  @Override
  public Object sparkSession() throws Exception {
    if (sparksession == null) {
      synchronized (this) {
        if (sparksession == null) {
          try {
            Class<?> clz = Class.forName("org.apache.spark.sql.SparkSession$");
            Object spark = clz.getField("MODULE$").get(null);
            Method m = clz.getMethod("builder");
            Object builder = m.invoke(spark);

            SparkConf conf = sc.getConf();
            if (conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase() == "hive") {
              if ((boolean) clz.getMethod("hiveClassesArePresent").invoke(spark)) {
                ClassLoader loader = Thread.currentThread().getContextClassLoader() != null ?
                  Thread.currentThread().getContextClassLoader() : getClass().getClassLoader();
                if (loader.getResource("hive-site.xml") == null) {
                  LOG.warn("livy.repl.enable-hive-context is true but no hive-site.xml found on " +
                   "classpath");
                }

                builder.getClass().getMethod("enableHiveSupport").invoke(builder);
                sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
                LOG.info("Created Spark session (with Hive support).");
              } else {
                sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
                LOG.info("Created Spark session.");
              }
            } else {
              sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
              LOG.info("Created Spark session.");
            }
          } catch (Exception e) {
            LOG.warn("SparkSession is not supported", e);
            throw e;
          }
        }
      }
    }

    return sparksession;
  }

  @Override
  public SQLContext sqlctx() {
    if (sqlctx == null) {
      synchronized (this) {
        if (sqlctx == null) {
          sqlctx = new SQLContext(sc);
        }
      }
    }
    return sqlctx;
  }

  @Override
  public HiveContext hivectx() {
    if (hivectx == null) {
      synchronized (this) {
        if (hivectx == null) {
          SparkConf conf = sc.getConf();
          if (conf.getBoolean("spark.repl.enableHiveContext", false)) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader() != null ?
              Thread.currentThread().getContextClassLoader() : getClass().getClassLoader();
            if (loader.getResource("hive-site.xml") == null) {
              LOG.warn("livy.repl.enable-hive-context is true but no hive-site.xml found on " +
               "classpath.");
            }
            hivectx = new HiveContext(sc.sc());
          }
        }
      }
    }
    return hivectx;
  }

  @Override
  public synchronized JavaStreamingContext streamingctx(){
    Utils.checkState(streamingctx != null, "method createStreamingContext must be called first.");
    return streamingctx;
  }

  @Override
  public synchronized void createStreamingContext(long batchDuration) {
    Utils.checkState(streamingctx == null, "Streaming context is not null.");
    streamingctx = new JavaStreamingContext(sc, new Duration(batchDuration));
  }

  @Override
  public synchronized void stopStreamingCtx() {
    Utils.checkState(streamingctx != null, "Streaming Context is null");
    streamingctx.stop();
    streamingctx = null;
  }

  @Override
  public File getLocalTmpDir() {
    return localTmpDir;
  }

  public synchronized void stop() {
    if (streamingctx != null) {
      stopStreamingCtx();
    }
    if (sc != null) {
      sc.stop();
    }
  }

  public void addFile(String path) {
    driver.addFile(path);
  }

  public void addJarOrPyFile(String path) throws Exception {
    driver.addJarOrPyFile(path);
  }
}
