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

package org.apache.datafusion.examples.presentation;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;

/**
 * Presentation example: SQL on registered tables. Each {@code === SLIDE ===} block is sized to fit
 * on a single presentation slide.
 */
public final class SqlSlides {

  private SqlSlides() {}

  public static void main(String[] args) throws Exception {
    Path csv = writeSampleCsv();

    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {

      // === SLIDE: Register a CSV file as a SQL table ===========================
      ctx.registerCsv("orders", csv.toString());
      try (DataFrame df = ctx.sql("SELECT priority, COUNT(*) AS n FROM orders GROUP BY priority")) {
        df.show();
      }
      // === END SLIDE ===========================================================

      // === SLIDE: Pull aggregated results into Arrow vectors ===================
      try (DataFrame df =
              ctx.sql(
                  "SELECT priority, CAST(SUM(amount) AS DOUBLE) AS total"
                      + " FROM orders GROUP BY priority ORDER BY total DESC");
          ArrowReader reader = df.collect(allocator)) {
        while (reader.loadNextBatch()) {
          VectorSchemaRoot batch = reader.getVectorSchemaRoot();
          VarCharVector p = (VarCharVector) batch.getVector("priority");
          Float8Vector total = (Float8Vector) batch.getVector("total");
          for (int i = 0; i < batch.getRowCount(); i++) {
            System.out.printf("%-7s %.2f%n", new String(p.get(i)), total.get(i));
          }
        }
      }
      // === END SLIDE ===========================================================

      // === SLIDE: Mix SQL and DataFrame — same engine, same plan ===============
      try (DataFrame eu = ctx.sql("SELECT priority, amount FROM orders WHERE region = 'EU'");
          DataFrame big = eu.filter("amount >= 50")) {
        big.show();
      }
      // === END SLIDE ===========================================================

      // === SLIDE: Pull a scalar out of a one-row result ========================
      try (DataFrame df = ctx.sql("SELECT COUNT(*) AS n FROM orders");
          ArrowReader reader = df.collect(allocator)) {
        reader.loadNextBatch();
        BigIntVector n = (BigIntVector) reader.getVectorSchemaRoot().getVector("n");
        System.out.println("rows = " + n.get(0));
      }
      // === END SLIDE ===========================================================
    } finally {
      Files.deleteIfExists(csv);
    }
  }

  private static Path writeSampleCsv() throws Exception {
    Path csv = Files.createTempFile("orders", ".csv");
    Files.writeString(
        csv,
        """
        priority,amount,region
        HIGH,100,US
        LOW,25,EU
        HIGH,40,US
        MEDIUM,60,US
        HIGH,75,EU
        LOW,25,EU
        """);
    return csv;
  }
}
