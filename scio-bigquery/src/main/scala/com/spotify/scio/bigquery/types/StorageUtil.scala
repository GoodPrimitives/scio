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

package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import scala.collection.JavaConverters._

/** Utility for BigQuery Storage API. */
object StorageUtil {

  // https://cloud.google.com/bigquery/docs/reference/storage/
  def toTableSchema(avroSchema: Schema): TableSchema = {
    val fields = getFieldSchemas(avroSchema)

    new TableSchema().setFields(fields.asJava)
  }

  private def getFieldSchemas(avroSchema: Schema): List[TableFieldSchema] =
    avroSchema.getFields.asScala.map(toTableFieldSchema).toList

  private def toTableFieldSchema(field: Schema.Field): TableFieldSchema = {
    val schema = field.schema
    val (mode, tpe) = schema.getType match {
      case Type.UNION =>
        val types = schema.getTypes
        assert(types.size == 2 && types.get(0).getType == Type.NULL)
        ("NULLABLE", types.get(1))
      case Type.ARRAY =>
        ("REPEATED", schema.getElementType)
      case _ =>
        ("REQUIRED", schema)
    }
    val tableField = new TableFieldSchema().setName(field.name).setMode(mode)
    setRawType(tableField, tpe)
    tableField
  }

  // scalastyle:off cyclomatic.complexity
  private def setRawType(tableField: TableFieldSchema, schema: Schema): Unit = {
    val tpe = schema.getType match {
      case Type.BOOLEAN => "BOOLEAN"
      case Type.LONG =>
        schema.getDoc match {
          case null => "INT64"
          // FIXME: is this what "Avro schema annotations" in the documentation means?
          case "logicalType:timestamp-micros" => "TIMESTAMP"
        }
      case Type.DOUBLE => "FLOAT64"
      case Type.BYTES  => "BYTES"
      case Type.INT =>
        assert(schema.getDoc == "logicalType:date")
        "DATE"
      case Type.STRING =>
        schema.getDoc match {
          case "sqlType:DATETIME"  => "DATETIME"
          case "sqlType:GEOGRAPHY" => "GEOGRAPHY"
          case null                => "STRING"
        }
      case Type.RECORD =>
        tableField.setFields(getFieldSchemas(schema).asJava)
        "RECORD"
    }
    tableField.setType(tpe)
  }
  // scalastyle:on cyclomatic.complexity

}
