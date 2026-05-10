/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datafusion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

final class TestParquet {

    private static final String SCHEMA_STR =
            "message People { required int32 id; required binary name (UTF8); }";

    private TestParquet() {}

    /** Writes a 3-row Parquet file ({@code (id INT32, name UTF8)}) at {@code file}. */
    static void writeTinyParquet(java.nio.file.Path file) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType(SCHEMA_STR);
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new Path(file.toUri()))
                .withConf(conf)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            writer.write(factory.newGroup().append("id", 1).append("name", "alice"));
            writer.write(factory.newGroup().append("id", 2).append("name", "bob"));
            writer.write(factory.newGroup().append("id", 3).append("name", "carol"));
        }
    }
}
