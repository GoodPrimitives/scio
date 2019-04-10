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

object StorageBenchmark {

  @BigQueryType.fromQuery(
    """
      |SELECT commit, author, subject
      |FROM `bigquery-public-data.github_repos.commits`
    """.stripMargin
  )
  class Query

  @BigQueryType.fromQuery(
    """
      |SELECT commit, author, subject
      |FROM `bigquery-public-data.github_repos.commits`
      |WHERE commit LIKE 'a%%'
    """.stripMargin
  )
  class QueryFilter

  @BigQueryType.fromStorage(
    "bigquery-public-data:github_repos.commits",
    selectedFields = List("commit", "author", "subject")
  )
  class Storage

  @BigQueryType.fromStorage(
    "bigquery-public-data:github_repos.commits",
    selectedFields = List("commit", "author", "subject"),
    rowRestriction = "commit LIKE 'a%%'"
  )
  class StorageFilter

  def main(cmdlineArgs: Array[String]): Unit = {
    val args = Array("--runner=DataflowRunner", "--numWorkers=8", "--autoscalingAlgorithm=NONE")

    val (sc1, _) = ContextAndArgs(args :+ "--appName=FromQuery")
    sc1.typedBigQuery[Query]().count
    sc1.close()

    val (sc2, _) = ContextAndArgs(args :+ "--appName=FromQueryFilter")
    sc2.typedBigQuery[QueryFilter]().count
    sc2.close()

    val (sc3, _) = ContextAndArgs(args :+ "--appName=FromStorage")
    sc3.typedBigQuery[Storage]().count
    sc3.close()

    val (sc4, _) = ContextAndArgs(args :+ "--appName=FromStorageFilter")
    sc4.typedBigQuery[StorageFilter]().count
    sc4.close()

    ()
  }

}
