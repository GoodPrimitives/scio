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

import com.spotify.scio.bigquery.types.BigQueryType

object StorageTest {

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.commits")
  class Record1

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.%s", List("commits"))
  class Record2

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.%s",
                            List("commits"),
                            List("commit", "committer.name", "difference"))
  class Record3

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.%s",
                            List("commits"),
                            List("commit", "committer.name", "difference"),
                            "committer.name LIKE 'Neville%'")
  class Record4

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.%s", args = List("commits"))
  class Record5

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.commits",
                            selectedFields = List("commit", "committer.name", "difference"))
  class Record6

  @BigQueryType.fromStorage("bigquery-public-data:github_repos.commits",
                            rowRestriction = "committer.name LIKE 'Neville%'")
  class Record7

  def main(args: Array[String]): Unit = {
    val bqt = BigQueryType[Record4]
    // scalastyle:off regex
    println(bqt.isStorage)
    println(bqt.selectedFields)
    println(bqt.rowRestriction)
  }
}
