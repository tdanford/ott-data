/**
 * Copyright 2014 Timothy Danford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package github.tdanford.ott

import github.tdanford.ott.rdd.OTTContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.StatsReportListener
import org.apache.hadoop.fs.Path

object Main extends App {

  override def main( args : Array[String] ) {

    val sc = createSparkContext("main", "local[8]")
    val lines = sc.loadTaxonomyFile(args(0))

    val first100 = lines.take(100)
    first100.foreach {
      case line : TaxonomyLine =>
        println("%s -> %s".format(line.uid, line.uniqname))
    }
  }

  def createSparkContext(name: String,
                         master: String,
                         sparkHome: String = null,
                         sparkJars: Seq[String] = Nil,
                         sparkEnvVars: Seq[(String, String)] = Nil,
                         sparkAddStatsListener: Boolean = false,
                         sparkKryoBufferSize: Int = 4,
                         loadSystemValues: Boolean = true,
                         sparkDriverPort: Option[Int] = None): SparkContext = {

    val config: SparkConf = new SparkConf(loadSystemValues).setAppName("ott: " + name).setMaster(master)
    if (sparkHome != null)
      config.setSparkHome(sparkHome)
    if (sparkJars != Nil)
      config.setJars(sparkJars)
    if (sparkEnvVars != Nil)
      config.setExecutorEnv(sparkEnvVars)

    // Optionally set the spark driver port
    sparkDriverPort match {
      case Some(port) => config.set("spark.driver.port", port.toString)
      case None       =>
    }

    // Setup the Kryo settings
    /*
    config.setAll(Array(("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
      ("spark.kryo.registrator", "org.bdgenomics.adam.serialization.ADAMKryoRegistrator"),
      ("spark.kryoserializer.buffer.mb", sparkKryoBufferSize.toString),
      ("spark.kryo.referenceTracking", "true")))
    */

    val sc = new SparkContext(config)

    if (sparkAddStatsListener) {
      sc.addSparkListener(new StatsReportListener)
    }

    sc
  }
}
