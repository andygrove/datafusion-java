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

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.datafusion.ColumnarValue;
import org.apache.datafusion.DataFrame;
import org.apache.datafusion.ScalarFunction;
import org.apache.datafusion.ScalarFunctionArgs;
import org.apache.datafusion.ScalarUdf;
import org.apache.datafusion.SessionContext;
import org.apache.datafusion.Volatility;

/**
 * Presentation example: a Java scalar UDF. Each {@code === SLIDE ===} block is sized to fit on a
 * single presentation slide.
 */
public final class ScalarUdfSlides {

  private ScalarUdfSlides() {}

  // === SLIDE: Declare the function — name, signature, volatility ===============
  public static final class Celsius implements ScalarFunction {
    private static final ArrowType F64 =
        new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);

    @Override
    public String name() {
      return "celsius";
    }

    @Override
    public List<Field> argFields() {
      return List.of(Field.nullable("f", F64));
    }

    @Override
    public Field returnField() {
      return Field.nullable("c", F64);
    }

    @Override
    public Volatility volatility() {
      return Volatility.IMMUTABLE;
    }

    @Override
    public ColumnarValue evaluate(BufferAllocator allocator, ScalarFunctionArgs args) {
      Float8Vector in = (Float8Vector) args.args().get(0).vector();
      Float8Vector out = new Float8Vector("c", allocator);
      out.allocateNew(in.getValueCount());
      for (int i = 0; i < in.getValueCount(); i++) {
        if (in.isNull(i)) out.setNull(i);
        else out.set(i, (in.get(i) - 32) * 5.0 / 9.0);
      }
      out.setValueCount(in.getValueCount());
      return ColumnarValue.array(out);
    }
  }

  // === END SLIDE ==============================================================

  public static void main(String[] args) throws Exception {
    try (SessionContext ctx = new SessionContext();
        BufferAllocator allocator = new RootAllocator()) {

      // === SLIDE: Register the UDF and call it from SQL =========================
      ctx.registerUdf(new ScalarUdf(new Celsius()));
      try (DataFrame df =
          ctx.sql("SELECT f, celsius(f) AS c FROM (VALUES (32.0), (212.0), (98.6)) AS t(f)")) {
        df.show();
      }
      // === END SLIDE ============================================================
    }
  }
}
