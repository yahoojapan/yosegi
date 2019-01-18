/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.blackbox;

import java.util.Map;
import java.util.HashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestPrimitiveSchema{

  @Test
  public void T_1() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    Spread s = new Spread();
    try (YosegiWriter writer = new YosegiWriter(out, config)) {
      Map<String, Object> d = new HashMap<String, Object>();
      d.put("d", new BooleanObj(true));
      s.addRow(d);
      s.addRow(d);
      s.addRow(d);
      s.addRow(d);
      d.put("d", new BooleanObj(false));
      s.addRow(d);
      d.put("d", new BooleanObj(true));
      s.addRow(d);
      s.addRow(d);
      s.addRow(d);
      s.addRow(d);
      d.put("d", null);
      s.addRow(d);
      d.put("d", new BooleanObj(true));
      s.addRow(d);
      s.addRow(d);
      d.put("d", null);
      s.addRow(d);
      d.put("d", new BooleanObj(true));
      s.addRow(d);
      d.put("d", null);
      s.addRow(d);
      writer.append(s);
    }

    assertEquals(s.getColumn("d").getColumnType(), ColumnType.BOOLEAN);

    try (YosegiReader reader = new YosegiReader()) {
      Configuration readerConfig = new Configuration();
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      while (reader.hasNext()) {
        Spread spread = reader.next();
        IColumn column = spread.getColumn("d");
        System.err.println(column.toString());
        assertEquals(column.getColumnType(), ColumnType.BOOLEAN);
      }
    }
  }
}
