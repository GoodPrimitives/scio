/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.spotify.scio._
import com.spotify.scio.bigquery._

object StorageTest {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.bigQueryStorage("bigquery-public-data:samples.github_nested",
                       List("actor", "actor_attributes.email", "payload"),
                       "actor LIKE 'neville%'")
      .debug()

    sc.close()
  }
}

object TypedStorageTest {

  @BigQueryType.fromStorage("bigquery-public-data:samples.github_nested",
                            selectedFields = List("actor", "actor_attributes.email"),
                            rowRestriction = "actor LIKE 'neville%'")
  class Record

  def main(cmdlineArgs: Array[String]): Unit = {
    val bqt = BigQueryType[Record]
    // scalastyle:off regex
    println(bqt.schema)
    println(Record.schema)

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.typedBigQuery[Record]()
      .debug()
    sc.close()
  }
}
