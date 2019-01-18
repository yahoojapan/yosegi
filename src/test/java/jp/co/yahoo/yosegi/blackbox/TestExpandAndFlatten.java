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

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.IColumn;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;

public class TestExpandAndFlatten{


  @Test
  public void T_1() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();


    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResource("blackbox/TestExpandAndFlatten.json").openStream()));
    String line = in.readLine();
    Spread writeSpread = new Spread();
    try (YosegiWriter writer = new YosegiWriter(out, config)) {
      while (line != null) {
        IParser parser = messageReader.create(line);
        writeSpread.addParserRow(parser);
        line = in.readLine();
      }
      writer.append(writeSpread);
      writer.append(writeSpread);
    }

    try (YosegiReader reader = new YosegiReader()) {
      Configuration readerConfig = new Configuration();
      readerConfig.set("spread.reader.expand.column", "{ \"base\" :{ \"node\" : \"col3\" ,  \"link_name\" : \"expand\" } }");
      readerConfig.set("spread.reader.flatten.column", "[ { \"link_name\" : \"f1\" , \"nodes\" : [\"expand\" , \"f1\"] } , { \"link_name\" : \"f2\" , \"nodes\" : [\"expand\" , \"f2\"] } ]");
      readerConfig.set("spread.reader.read.column.names", "[ [ \"f1\"] , [ \"f2\"] , [ \"f3\"] , [\"col1\"] , [ \"a\" , \"b\" ] ]");
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      while (reader.hasNext()) {
        Spread spread = reader.next();
        IColumn f1Column = spread.getColumn("f1");
        IColumn f2Column = spread.getColumn("f2");
        IColumn f3Column = spread.getColumn("f3");
        IColumn col1Column = spread.getColumn("col1");
        for (int i = 0; i < spread.size(); i++) {
          assertEquals(((PrimitiveObject) (f1Column.get(i).getRow())).getString(), "f1");
          assertEquals(((PrimitiveObject) (f2Column.get(i).getRow())).getString(), "f2");
          assertEquals(f3Column.get(i).getRow(), null);
          assertEquals(col1Column.get(i).getRow(), null);
        }
        System.out.println(spread.toString());
      }
    }
  }
}
