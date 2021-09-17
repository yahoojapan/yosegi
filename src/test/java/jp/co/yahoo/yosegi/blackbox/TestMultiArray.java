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

import jp.co.yahoo.yosegi.inmemory.SpreadRawConverter;
import jp.co.yahoo.yosegi.reader.WrapReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.expression.*;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestMultiArray{
  private ByteArrayOutputStream out;

  @BeforeEach
  public void setup() throws IOException{
    out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    try(YosegiRecordWriter writer = new YosegiRecordWriter(out , config )) {

      JacksonMessageReader messageReader = new JacksonMessageReader();
      BufferedReader in = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResource("blackbox/TestMultiArray.json").openStream()));
      String line = in.readLine();
      while (line != null) {
        IParser parser = messageReader.create(line);
        writer.addParserRow(parser);
        line = in.readLine();
      }
    }
  }

  @Test
  public void T_1() throws IOException{
    try(YosegiReader reader = new YosegiReader()) {
      Configuration readerConfig = new Configuration();
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        IColumn unionColumn = spread.getColumn("array1");
        assertEquals(unionColumn.getColumnType(), ColumnType.ARRAY);
      }
    }
  }

  @Test
  public void T_2() throws IOException{
    YosegiReader reader = new YosegiReader();
    Configuration readerConfig = new Configuration();
    readerConfig.set("spread.reader.expand.column", "{ \"base\" : { \"node\" : \"array1\" , \"link_name\" : \"expand_array1\", \"child_node\" : { \"node\" : \"array2\"  , \"link_name\" : \"expand_array2\"  } } }");
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream(data);
    reader.setNewStream(fileIn, data.length, readerConfig);
    WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
    while (spreadWrapReader.hasNext()) {
      Spread spread = spreadWrapReader.next();
      IColumn spreadColumn = spread.getColumn("expand_array2");
      assertEquals(spreadColumn.getColumnType(), ColumnType.SPREAD);
      IColumn stringColumn = spreadColumn.getColumn("array2-string");
      assertEquals(3, stringColumn.size());

      assertEquals("a", ((PrimitiveObject) (stringColumn.get(0).getRow())).getString());
      assertEquals("b", ((PrimitiveObject) (stringColumn.get(1).getRow())).getString());
      assertEquals("c", ((PrimitiveObject) (stringColumn.get(2).getRow())).getString());
      assertEquals(null, stringColumn.get(3).getRow());
    }
  }

  @Test
  public void T_3() throws IOException{
    try(YosegiReader reader = new YosegiReader()) {
      Configuration readerConfig = new Configuration();
      readerConfig.set("spread.reader.expand.column", "{ \"base\" : { \"node\" : \"array1\" , \"link_name\" : \"expand_array1\", \"child_node\" : { \"node\" : \"array2\"  , \"link_name\" : \"expand_array2\"  } } }");
      readerConfig.set("spread.reader.flatten.column", "[ { \"link_name\" : \"string\" , \"nodes\" : [\"expand_array2\" , \"array2-string\"] } , { \"link_name\" : \"integer\" , \"nodes\" : [\"expand_array2\" , \"array2-integer\"] } ]");
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        IColumn stringColumn = spread.getColumn("string");
        assertEquals(3, stringColumn.size());
        IColumn integerColumn = spread.getColumn("integer");
        assertEquals(3, integerColumn.size());

        assertEquals("a", ((PrimitiveObject) (stringColumn.get(0).getRow())).getString());
        assertEquals("b", ((PrimitiveObject) (stringColumn.get(1).getRow())).getString());
        assertEquals("c", ((PrimitiveObject) (stringColumn.get(2).getRow())).getString());
        assertEquals(null, stringColumn.get(3).getRow());
        assertEquals(1, ((PrimitiveObject) (integerColumn.get(0).getRow())).getInt());
        assertEquals(2, ((PrimitiveObject) (integerColumn.get(1).getRow())).getInt());
        assertEquals(3, ((PrimitiveObject) (integerColumn.get(2).getRow())).getInt());
        assertEquals(null, stringColumn.get(3).getRow());
      }
    }
  }

  @Test
  public void T_6() throws IOException{
    try(YosegiReader reader = new YosegiReader()) {
      Configuration readerConfig = new Configuration();
      readerConfig.set("spread.reader.expand.column", "{ \"base\" : { \"node\" : \"array1\" , \"link_name\" : \"expand_array1\", \"child_node\" : { \"node\" : \"spread\"  , \"child_node\" : { \"node\" : \"array2\"  ,  \"link_name\" : \"expand_array2\" , \"child_node\" : { \"node\" : \"array3\"  ,  \"link_name\" : \"expand_array3\" }  }  } } }");
      byte[] data = out.toByteArray();
      InputStream fileIn = new ByteArrayInputStream(data);
      reader.setNewStream(fileIn, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        IColumn stringColumn = spread.getColumn("expand_array3");
        assertEquals(3, stringColumn.size());

        assertEquals(1, ((PrimitiveObject) (stringColumn.get(0).getRow())).getInt());
        assertEquals(2, ((PrimitiveObject) (stringColumn.get(1).getRow())).getInt());
        assertEquals(3, ((PrimitiveObject) (stringColumn.get(2).getRow())).getInt());
        assertEquals(null, stringColumn.get(3).getRow());
      }
    }
  }

}
