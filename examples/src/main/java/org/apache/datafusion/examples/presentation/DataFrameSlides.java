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

import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;

/**
 * Presentation example: the DataFrame API. Each {@code === SLIDE ===} block is sized to fit on a
 * single presentation slide.
 */
public final class DataFrameSlides {

  private DataFrameSlides() {}

  public static void main(String[] args) throws Exception {
    Path csv = writeSampleCsv();
    Path parquetDir = Files.createTempDirectory("df-slides-parquet");

    try (SessionContext ctx = new SessionContext()) {

      // === SLIDE: Read a CSV, filter, project, show ============================
      try (DataFrame orders = ctx.readCsv(csv.toString());
          DataFrame highValue = orders.filter("amount > 50").select("region", "amount")) {
        highValue.show();
      }
      // === END SLIDE ===========================================================

      // === SLIDE: Chain transformations, then count ============================
      try (DataFrame regions =
          ctx.readCsv(csv.toString()).filter("priority = 'HIGH'").select("region").distinct()) {
        System.out.println("distinct HIGH-priority regions = " + regions.count());
      }
      // === END SLIDE ===========================================================

      // === SLIDE: Round-trip through Parquet ===================================
      try (DataFrame source = ctx.readCsv(csv.toString())) {
        source.writeParquet(parquetDir.toString());
      }
      try (DataFrame back = ctx.readParquet(parquetDir.toString())) {
        back.show();
      }
      // === END SLIDE ===========================================================
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
