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
package github.tdanford.ott.rdd

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import github.tdanford.ott.{OTTTaxonomyLoader, TaxonomyLine}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object OTTContext {
  implicit def contextToOTTContext( sc : SparkContext ) : OTTContext = new OTTContext(sc)
}

class OTTContext( sc : SparkContext ) extends Serializable with Logging {

  def loadTaxonomyFile( path : String ) : RDD[TaxonomyLine] = {
    val conf = sc.hadoopConfiguration
    //val job = new Job(conf)
    val records : RDD[(LongWritable, Text)] =
      sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](path,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text], conf)

    val tax : RDD[TaxonomyLine] = records.map(_._2.toString).map(OTTTaxonomyLoader.parseLine)
    tax
  }
}
