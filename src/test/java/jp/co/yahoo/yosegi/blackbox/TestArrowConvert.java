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
import jp.co.yahoo.yosegi.writer.YosegiArrowWriter;
import jp.co.yahoo.yosegi.reader.YosegiArrowReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestArrowConvert {

  private byte[] getYosegiFile() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    Spread s = new Spread();
    try (YosegiWriter writer = new YosegiWriter(out, config)) {
      Map<String, Object> d = new HashMap<String, Object>();
      Map<String, Object> d2 = new HashMap<String, Object>();
      d.put("d", new BooleanObj(true));
      d2.put("d", new BooleanObj(false));
      s.addRow(d);
      s.addRow(d2);
      s.addRow(d);
      s.addRow(d2);
      s.addRow(d);
      s.addRow(d2);
      s.addRow(d);
      s.addRow(d2);
      s.addRow(d);
      s.addRow(d2);
      writer.append(s);
      writer.append(s);
      writer.append(s);
      writer.append(s);
      writer.append(s);
      writer.close();
    }
    return out.toByteArray();
  }

  @Test
  public void T_1() throws IOException {
    byte[] yosegi = getYosegiFile();
    YosegiArrowReader arrowReader =
        YosegiArrowReader.newInstance( new ByteArrayInputStream( yosegi ) , yosegi.length , new Configuration() );
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    YosegiArrowWriter arrowWriter = new YosegiArrowWriter( out , new Configuration() );
    while ( arrowReader.hasNext() ) {
      arrowWriter.append( arrowReader.nextToBytes() );
    }
    arrowWriter.close();
    byte[] newYosegi = out.toByteArray();
    YosegiReader reader = new YosegiReader();
    reader.setNewStream( new ByteArrayInputStream( newYosegi ) , newYosegi.length , new Configuration() );
    int count = 0;
    while ( reader.hasNext() ) {
      Spread s = reader.next();
      IColumn c = s.getColumn( "d" );
      for( int i = 0 ; i < 10 ; i+=2 ) {
        assertTrue( ( (PrimitiveObject)( c.get( i ).getRow() ) ).getBoolean() );
        assertFalse( ( (PrimitiveObject)( c.get( i + 1 ).getRow() ) ).getBoolean() );
      }
      count++;
    }
    assertEquals( count , 5 );
  }
}
