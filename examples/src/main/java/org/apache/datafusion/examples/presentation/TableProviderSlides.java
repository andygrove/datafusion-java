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

import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.SimpleTableProvider;
import org.apache.datafusion.TableProvider;

/**
 * Presentation example: a custom Java {@link TableProvider}. Each {@code === SLIDE ===} block is
 * sized to fit on a single presentation slide.
 *
 * <p>The provider exposes a single {@code id INT} column with values {@code 0 .. n-1}.
 */
public final class TableProviderSlides {

  private TableProviderSlides() {}

  // === SLIDE: A TableProvider is just a schema + a scan() ======================
  public static TableProvider numbers(int n) {
    Schema schema = new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true))));
    return new SimpleTableProvider(schema, allocator -> new RangeReader(allocator, schema, n));
  }

  // === END SLIDE ==============================================================

  // === SLIDE: scan() returns an ArrowReader — produce one batch ================
  static final class RangeReader extends ArrowReader {
    private final Schema schema;
    private final int n;
    private boolean delivered;

    RangeReader(BufferAllocator allocator, Schema schema, int n) {
      super(allocator);
      this.schema = schema;
      this.n = n;
    }

    @Override
    protected Schema readSchema() {
      return schema;
    }

    @Override
    public long bytesRead() {
      return 0L;
    }

    @Override
    protected void closeReadSource() {}

    @Override
    public boolean loadNextBatch() throws IOException {
      if (delivered) return false;
      VectorSchemaRoot root = getVectorSchemaRoot();
      IntVector id = (IntVector) root.getVector("id");
      id.allocateNew(n);
      for (int i = 0; i < n; i++) id.set(i, i);
      id.setValueCount(n);
      root.setRowCount(n);
      delivered = true;
      return true;
    }
  }

  // === END SLIDE ==============================================================

  public static void main(String[] args) throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {

      // === SLIDE: Register, then query with SQL or DataFrame ===================
      ctx.registerTable("nums", numbers(10));
      try (DataFrame df = ctx.sql("SELECT SUM(id) AS total, COUNT(*) AS n FROM nums")) {
        df.show();
      }
      // === END SLIDE ===========================================================
    }
  }
}
