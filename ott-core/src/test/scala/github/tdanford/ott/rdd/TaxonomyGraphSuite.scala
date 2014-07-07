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

import OTTContext._
import github.tdanford.ott.SparkFunSuite

class TaxonomyGraphSuite extends SparkFunSuite {

  sparkTest("can create a graph from the test taxonomy file") {
    val p : String = path("test-taxonomy.tsv")
    val lines = sc.loadTaxonomyFile(p)
    val g = TaxonomyGraph.asGraph(lines)
    assert(g.numVertices === 19)
    assert(g.numEdges === 18*2)
  }

  sparkTest("can measure simple parent/child distance in the graph") {
    val p : String = path("test-taxonomy.tsv")
    val lines = sc.loadTaxonomyFile(p)
    val g = TaxonomyGraph.asGraph(lines)
    val dist = TaxonomyGraph.findDistance(g, "4917536", "805080")
    assert(dist === 2)
  }

  sparkTest("can find the path between a parent/child in the graph") {
    val p : String = path("test-taxonomy.tsv")
    val lines = sc.loadTaxonomyFile(p)
    val g = TaxonomyGraph.asGraph(lines)
    val gp = TaxonomyGraph.findPath(g, "4917536", "805080")
    assert(gp.length() === 2)
    assert(gp.firstNode === 805080)
    assert(gp.lastNode === 4917536)
  }

  sparkTest("can find the path between a child/parent in the graph") {
    val p : String = path("test-taxonomy.tsv")
    val lines = sc.loadTaxonomyFile(p)
    val g = TaxonomyGraph.asGraph(lines)
    val gp = TaxonomyGraph.findPath(g, "805080", "4917536")
    assert(gp.length() === 2)
    assert(gp.lastNode === 805080)
    assert(gp.firstNode === 4917536)
  }

  // Homo sapiens is 770315
  // Homo sapiens sapiens is 3607676
  // Root is 805030
  // Mus musculus is 542509
}
