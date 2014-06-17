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

import org.scalatest.FunSuite
import java.io.File

class OTTTaxonomyLoaderSuite extends FunSuite {

  test("correctly finds the set of columns in the test-taxonomy.tsv file") {
    val testPath = Thread.currentThread().getContextClassLoader.getResource("test-taxonomy.tsv").getFile
    val testFile = new File(testPath)
    val loader = new OTTTaxonomyLoader(testFile)
    assert(loader.headerFields.length === 7)
  }

  test("correctly load 19 lines from the test-taxonomy.tsv file") {
    val testPath = Thread.currentThread().getContextClassLoader.getResource("test-taxonomy.tsv").getFile
    val testFile = new File(testPath)
    val loader = new OTTTaxonomyLoader(testFile)
    assert(loader.length === 19)
  }

  test("show that first line in test-taxonomy.tsv file has name 'life' and uid '805080'") {
    val testPath = Thread.currentThread().getContextClassLoader.getResource("test-taxonomy.tsv").getFile
    val testFile = new File(testPath)
    val loader = new OTTTaxonomyLoader(testFile)
    val line = loader.next()
    assert(line.uid === "805080")
    assert(line.name === "life")
  }
}
