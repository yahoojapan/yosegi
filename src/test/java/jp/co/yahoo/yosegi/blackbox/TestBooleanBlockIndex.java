/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.blackbox;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.inmemory.SpreadRawConverter;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.reader.WrapReader;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.BooleanFilter;
import jp.co.yahoo.yosegi.spread.expression.AndExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.ExecuterNode;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.NotExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.OrExpressionNode;
import jp.co.yahoo.yosegi.spread.expression.StringExtractNode;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestBooleanBlockIndex {

  /*
   col1: true,true,true
   col2: false,false,false
   col3: true,true,false
   col4: true,false,true
   col5: false,false,true
   col6: false,true,false
   col7: true,(null),(null)
   col8: false,(null),(null)
   col9: true,true,(null)
   col10: true,false,(null)
   col11: false,false,(null)
   col12: false, true,(null)
   col13: true,(null),true
   col14: true,(null),false
   col15: false,(null),false
   col16: false,(null),true
  */
  public byte[] getData() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration writerConfig = new Configuration();

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader jsonIn =
        new BufferedReader(
            new InputStreamReader(
                this.getClass()
                    .getClassLoader()
                    .getResource("blackbox/TestBooleanBlockIndex.json")
                    .openStream()));
    String line = jsonIn.readLine();
    Spread writeSpread = new Spread();
    try (YosegiWriter writer = new YosegiWriter(out, writerConfig)) {
      while (line != null) {
        IParser parser = messageReader.create(line);
        writeSpread.addParserRow(parser);
        line = jsonIn.readLine();
      }
      writer.append(writeSpread);
    }

    return out.toByteArray();
  }

  public static Stream<Arguments> D_blackbox_1() {
    return Stream.of(
        // list{list{columnName, columnValues}, list{columnName, columnValues} ...}
        arguments(
            new ArrayList<List<Object>>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_1")
  public void T_blackbox_1(final List<List<Object>> expecteds) throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setNewStream(in, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        for (List<Object> expected : expecteds) {
          String columnName = (String) expected.get(0);
          IColumn col = spread.getColumn(columnName);
          for (int i = 0; i < spread.size(); i++) {
            List<Boolean> expectedValues = (List<Boolean>) expected.get(1);
            if (expectedValues.get(i) == null) {
              assertEquals(ColumnType.NULL, col.get(i).getType());
            } else {
              assertEquals(
                  expectedValues.get(i), ((PrimitiveObject) col.get(i).getRow()).getBoolean());
            }
          }
        }
      }
    }
  }

  public static Stream<Arguments> D_blackbox_2() {
    return Stream.of(
        // columnName, filterValue, expected(hasNext)
        arguments("col1", true, true),
        arguments("col1", false, false),
        arguments("col2", true, false),
        arguments("col2", false, true),
        arguments("col3", true, true),
        arguments("col3", false, true),
        arguments("col4", true, true),
        arguments("col4", false, true),
        arguments("col5", true, true),
        arguments("col6", false, true),
        arguments("col7", true, true),
        arguments("col7", false, false),
        arguments("col8", true, false),
        arguments("col8", false, true),
        arguments("col9", true, true),
        arguments("col9", false, false),
        arguments("col10", true, true),
        arguments("col10", false, true),
        arguments("col11", true, false),
        arguments("col11", false, true),
        arguments("col12", true, true),
        arguments("col12", false, true),
        arguments("col13", true, true),
        arguments("col13", false, false),
        arguments("col14", true, true),
        arguments("col14", false, true),
        arguments("col15", true, false),
        arguments("col15", false, true),
        arguments("col16", true, true),
        arguments("col16", false, true));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_2")
  public void T_blackbox_2(
      final String columnName, final Boolean filterValue, final Boolean expected)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new AndExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName), new BooleanFilter(filterValue)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      assertEquals(expected, reader.hasNext());
    }
  }

  public static Stream<Arguments> D_blackbox_3() {
    return Stream.of(
        // columnName1, filterValue1, columnName2, filterValue2, expected(hasNext)
        arguments("col1", true, "col2", true, false),
        arguments("col1", true, "col2", false, true),
        arguments("col1", false, "col2", false, false),
        arguments("col1", false, "col2", true, false));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_3")
  public void T_blackbox_3(
      final String columnName1,
      final Boolean filterValue1,
      final String columnName2,
      final Boolean filterValue2,
      final Boolean expected)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new AndExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName1), new BooleanFilter(filterValue1)));
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName2), new BooleanFilter(filterValue2)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      assertEquals(expected, reader.hasNext());
    }
  }

  public static Stream<Arguments> D_blackbox_4() {
    return Stream.of(
        // columnName1, filterValue1, columnName2, filterValue2, list{list{columnName,
        // columnValues}, list{columnName, columnValues} ...}
        arguments(
            "col1",
            true,
            "col2",
            false,
            new ArrayList<>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_4")
  public void T_blackbox_4(
      final String columnName1,
      final Boolean filterValue1,
      final String columnName2,
      final Boolean filterValue2,
      List<List<Object>> expecteds)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new AndExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName1), new BooleanFilter(filterValue1)));
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName2), new BooleanFilter(filterValue2)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        for (List<Object> expected : expecteds) {
          String columnName = (String) expected.get(0);
          IColumn col = spread.getColumn(columnName);
          for (int i = 0; i < spread.size(); i++) {
            List<Boolean> expectedValues = (List<Boolean>) expected.get(1);
            if (expectedValues.get(i) == null) {
              assertEquals(ColumnType.NULL, col.get(i).getType());
            } else {
              assertEquals(
                  expectedValues.get(i), ((PrimitiveObject) col.get(i).getRow()).getBoolean());
            }
          }
        }
      }
    }
  }

  public static Stream<Arguments> D_blackbox_5() {
    return Stream.of(
        // columnName, filterValue, expected(hasNext)
        arguments("col1", true, true),
        arguments("col1", false, false),
        arguments("col2", true, false),
        arguments("col2", false, true),
        arguments("col3", true, true),
        arguments("col3", false, true),
        arguments("col4", true, true),
        arguments("col4", false, true),
        arguments("col5", true, true),
        arguments("col6", false, true),
        arguments("col7", true, true),
        arguments("col7", false, false),
        arguments("col8", true, false),
        arguments("col8", false, true),
        arguments("col9", true, true),
        arguments("col9", false, false),
        arguments("col10", true, true),
        arguments("col10", false, true),
        arguments("col11", true, false),
        arguments("col11", false, true),
        arguments("col12", true, true),
        arguments("col12", false, true),
        arguments("col13", true, true),
        arguments("col13", false, false),
        arguments("col14", true, true),
        arguments("col14", false, true),
        arguments("col15", true, false),
        arguments("col15", false, true),
        arguments("col16", true, true),
        arguments("col16", false, true));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_5")
  public void T_blackbox_5(
      final String columnName, final Boolean filterValue, final Boolean expected)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new OrExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName), new BooleanFilter(filterValue)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      assertEquals(expected, reader.hasNext());
    }
  }

  public static Stream<Arguments> D_blackbox_6() {
    return Stream.of(
        // columnName1, filterValue1, columnName2, filterValue2, expected(hasNext)
        arguments("col1", true, "col2", true, true),
        arguments("col1", true, "col2", false, true),
        arguments("col1", false, "col2", false, true),
        arguments("col1", false, "col2", true, false));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_6")
  public void T_blackbox_6(
      final String columnName1,
      final Boolean filterValue1,
      final String columnName2,
      final Boolean filterValue2,
      final Boolean expected)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new OrExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName1), new BooleanFilter(filterValue1)));
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName2), new BooleanFilter(filterValue2)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      assertEquals(expected, reader.hasNext());
    }
  }

  public static Stream<Arguments> D_blackbox_7() {
    return Stream.of(
        // columnName1, filterValue1, columnName2, filterValue2, list{list{columnName,
        // columnValues}, list{columnName, columnValues} ...}
        arguments(
            "col1",
            true,
            "col2",
            false,
            new ArrayList<>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))),
        arguments(
            "col1",
            true,
            "col2",
            true,
            new ArrayList<>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))),
        arguments(
            "col1",
            false,
            "col2",
            false,
            new ArrayList<>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_7")
  public void T_blackbox_7(
      final String columnName1,
      final Boolean filterValue1,
      final String columnName2,
      final Boolean filterValue2,
      List<List<Object>> expecteds)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new OrExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName1), new BooleanFilter(filterValue1)));
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName2), new BooleanFilter(filterValue2)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        for (List<Object> expected : expecteds) {
          String columnName = (String) expected.get(0);
          IColumn col = spread.getColumn(columnName);
          for (int i = 0; i < spread.size(); i++) {
            List<Boolean> expectedValues = (List<Boolean>) expected.get(1);
            if (expectedValues.get(i) == null) {
              assertEquals(ColumnType.NULL, col.get(i).getType());
            } else {
              assertEquals(
                  expectedValues.get(i), ((PrimitiveObject) col.get(i).getRow()).getBoolean());
            }
          }
        }
      }
    }
  }

  public static Stream<Arguments> D_blackbox_8() {
    return Stream.of(
        // NOTE: NotExpressionNode does not work.
        // columnName, filterValue, expected(hasNext)
        arguments("col1", true, true), // false
        arguments("col1", false, true),
        arguments("col2", true, true),
        arguments("col2", false, true), // false
        arguments("col3", true, true), // false
        arguments("col3", false, true), // false
        arguments("col4", true, true), // false
        arguments("col4", false, true), // false
        arguments("col5", true, true), // false
        arguments("col6", false, true), // false
        arguments("col7", true, true), // false
        arguments("col7", false, true),
        arguments("col8", true, true),
        arguments("col8", false, true),
        arguments("col9", true, true), // false
        arguments("col9", false, true),
        arguments("col10", true, true), // false
        arguments("col10", false, true), // false
        arguments("col11", true, true),
        arguments("col11", false, true), // false
        arguments("col12", true, true), // false
        arguments("col12", false, true), // false
        arguments("col13", true, true), // false
        arguments("col13", false, true),
        arguments("col14", true, true), // false
        arguments("col14", false, true), // false
        arguments("col15", true, true),
        arguments("col15", false, true), // false
        arguments("col16", true, true), // false
        arguments("col16", false, true)); // false
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_8")
  public void T_blackbox_8(
      final String columnName, final Boolean filterValue, final Boolean expected)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new NotExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName), new BooleanFilter(filterValue)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      assertEquals(expected, reader.hasNext());
    }
  }

  public static Stream<Arguments> D_blackbox_9() {
    return Stream.of(
        // columnName, filterValue, list{list{columnName, columnValues}, list{columnName,
        // columnValues} ...}
        arguments(
            "col1",
            true,
            new ArrayList<>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))),
        arguments(
            "col1",
            false,
            new ArrayList<>(
                Arrays.asList(
                    new ArrayList<>(
                        Arrays.asList("col1", new ArrayList<>(Arrays.asList(true, true, true)))),
                    new ArrayList<>(
                        Arrays.asList("col2", new ArrayList<>(Arrays.asList(false, false, false)))),
                    new ArrayList<>(
                        Arrays.asList("col3", new ArrayList<>(Arrays.asList(true, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col4", new ArrayList<>(Arrays.asList(true, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col5", new ArrayList<>(Arrays.asList(false, false, true)))),
                    new ArrayList<>(
                        Arrays.asList("col6", new ArrayList<>(Arrays.asList(false, true, false)))),
                    new ArrayList<>(
                        Arrays.asList("col7", new ArrayList<>(Arrays.asList(true, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col8", new ArrayList<>(Arrays.asList(false, null, null)))),
                    new ArrayList<>(
                        Arrays.asList("col9", new ArrayList<>(Arrays.asList(true, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col10", new ArrayList<>(Arrays.asList(true, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col11", new ArrayList<>(Arrays.asList(false, false, null)))),
                    new ArrayList<>(
                        Arrays.asList("col12", new ArrayList<>(Arrays.asList(false, true, null)))),
                    new ArrayList<>(
                        Arrays.asList("col13", new ArrayList<>(Arrays.asList(true, null, true)))),
                    new ArrayList<>(
                        Arrays.asList("col14", new ArrayList<>(Arrays.asList(true, null, false)))),
                    new ArrayList<>(
                        Arrays.asList("col15", new ArrayList<>(Arrays.asList(false, null, false)))),
                    new ArrayList<>(
                        Arrays.asList(
                            "col16", new ArrayList<>(Arrays.asList(false, null, true))))))));
  }

  @ParameterizedTest
  @MethodSource("D_blackbox_9")
  public void T_blackbox_9(
      final String columnName, final Boolean filterValue, List<List<Object>> expecteds)
      throws IOException {
    try (YosegiReader reader = new YosegiReader()) {
      IExpressionNode index = new NotExpressionNode();
      index.addChildNode(
          new ExecuterNode(new StringExtractNode(columnName), new BooleanFilter(filterValue)));
      Configuration readerConfig = new Configuration();
      byte[] data = getData();
      InputStream in = new ByteArrayInputStream(data);
      reader.setBlockSkipIndex(index);
      reader.setNewStream(in, data.length, readerConfig);
      WrapReader<Spread> spreadWrapReader = new WrapReader<Spread>(reader, new SpreadRawConverter());
      while (spreadWrapReader.hasNext()) {
        Spread spread = spreadWrapReader.next();
        for (List<Object> expected : expecteds) {
          String colName = (String) expected.get(0);
          IColumn col = spread.getColumn(colName);
          for (int i = 0; i < spread.size(); i++) {
            List<Boolean> expectedValues = (List<Boolean>) expected.get(1);
            if (expectedValues.get(i) == null) {
              assertEquals(ColumnType.NULL, col.get(i).getType());
            } else {
              assertEquals(
                  expectedValues.get(i), ((PrimitiveObject) col.get(i).getRow()).getBoolean());
            }
          }
        }
      }
    }
  }
}
