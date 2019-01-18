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
package jp.co.yahoo.yosegi.spread;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.NullColumn;

public class TestSpread {

    public static Stream<Arguments> D_T_addRowNotException() {
        return Stream.of(
          arguments( new BooleanObj(false) ),
          arguments( new ByteObj((byte) 0) ),
          arguments( new BytesObj(new byte[0]) ),
          arguments( new DoubleObj((double) 0) ),
          arguments( new FloatObj((float) 0) ),
          arguments( new IntegerObj(0) ),
          arguments( new LongObj((long) 0) ),
          arguments( new ShortObj((short) 0) ),
          arguments( new StringObj("") ),
          arguments( new HashMap<String, Object>() ),
          arguments( new ArrayList<Object>() )
        );
    }

    public static Stream<Arguments> D_T_addRowException() {
        return Stream.of(
          arguments( "aaa" ),
          arguments( 0 )
        );
    }

    @Test
    public void T_constructTest_1() {
        Spread spread = new Spread();
    }

    @Test
    public void T_constructTest_2() {
        Spread spread = new Spread(NullColumn.getInstance());
    }

    @ParameterizedTest
    @MethodSource( "D_T_addRowException" )
    public void T_addRowException(final Object target) throws IOException {
        Spread spread = new Spread();
        assertThrows( IOException.class ,
          () -> {
            spread.addRow("col1", target);
          }
        );
    }

    @Test
    public void T_addRows() throws IOException {
        ArrayList<Map<String,Object>> rows = new ArrayList<>();
        Map<String, Object> dataContainer0 = new HashMap<String, Object>();
        Map<String, Object> dataContainer1 = new HashMap<String, Object>();
        Map<String, Object> dataContainer2 = new HashMap<String, Object>();
        dataContainer0.put("strColumn", new StringObj("row0"));
        dataContainer1.put("strColumn", new StringObj("row1"));
        dataContainer2.put("strColumn", new StringObj("row2"));
        dataContainer0.put("intColumn", new IntegerObj(0));
        dataContainer1.put("intColumn", new IntegerObj(1));
        dataContainer2.put("intColumn", new IntegerObj(2));
        rows.add(dataContainer0);
        rows.add(dataContainer1);
        rows.add(dataContainer2);
        Spread spread = new Spread();
        spread.addRows(rows);
        assertEquals(spread.getColumnSize(), 2);
        assertEquals(spread.getColumn("strColumn").get(0).toString(), "(STRING)row0");
        assertEquals(spread.getColumn("strColumn").get(1).toString(), "(STRING)row1");
        assertEquals(spread.getColumn("strColumn").get(2).toString(), "(STRING)row2");
        assertEquals(spread.getColumn("intColumn").get(0).toString(), "(INTEGER)0");
        assertEquals(spread.getColumn("intColumn").get(1).toString(), "(INTEGER)1");
        assertEquals(spread.getColumn("intColumn").get(2).toString(), "(INTEGER)2");
    }

    @ParameterizedTest
    @MethodSource( "D_T_addRowNotException")
    public void T_getAllColumnNotException(final Object target) throws IOException {
        Spread spread = new Spread();
        spread.addRow("col1", target);
        spread.getAllColumn();
    }

    @Test
    public void T_getAllColumn() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("boolean", new BooleanObj(false));
        dataContainer.put("byte",    new ByteObj((byte) 0));
        dataContainer.put("bytes",   new BytesObj(new byte[0]));
        dataContainer.put("double",  new DoubleObj((double) 0));
        dataContainer.put("float",   new FloatObj((float) 0));
        dataContainer.put("integer", new IntegerObj(5));
        dataContainer.put("long",    new LongObj((long) 0));
        dataContainer.put("short",   new ShortObj((short) 0));
        dataContainer.put("string",  new StringObj("val0"));
        Spread spread = new Spread();
        spread.addRow(dataContainer);
        assertEquals(spread.getAllColumn().size(), 9);
        assertEquals(spread.getAllColumn().get("string").get(0).toString(), "(STRING)val0");
        assertEquals(spread.getAllColumn().containsKey("double"), true);
        assertEquals(spread.getAllColumn().containsKey("hogehoge"), false);
    }

    @ParameterizedTest
    @MethodSource( "D_T_addRowNotException")
    public void T_getColumnNotException(final Object target) throws IOException {
        Spread spread = new Spread();
        spread.addRow("col1", target);
        spread.getColumn("col1");
    }

    @Test
    public void T_getColumn() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("string",  new StringObj("val0"));
        Spread spread = new Spread();
        spread.addRow(dataContainer);
        assertEquals(spread.getColumn("string").get(0).toString(), "(STRING)val0");
        assertEquals(spread.getColumn(0).get(0).toString(), "(STRING)val0");
        assertEquals(spread.getColumn(1),NullColumn.getInstance());
    }

    @Test
    public void T_toString() throws IOException {
        Map<String, Object> dataContainer = new HashMap<String, Object>();
        dataContainer.put("stringColumn",  new StringObj("val0"));
        dataContainer.put("intColumn",  new IntegerObj(123));
        Spread spread = new Spread();
        spread.addRow(dataContainer);
        assertEquals(spread.toString(),
                  "--------------------------\n"
                + "LINE-0\n"
                + "--------------------------\n"
                + "{stringColumn=(STRING)val0, intColumn=(INTEGER)123}\n"
                );
    }

}
