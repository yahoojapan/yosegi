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

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.BooleanBlockIndex;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.BooleanColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.BooleanColumnAnalizer;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizer;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.filter.BooleanFilter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TestFlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker {

  private IColumn makeColumn(final String columnName, Boolean[] values) throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BOOLEAN, columnName);
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        column.add(ColumnType.BOOLEAN, new BooleanObj(values[i]), i);
      }
    }
    return column;
  }

  private ColumnBinary makeColumnBinary(final String columnName, Boolean[] values)
      throws IOException {
    IColumn column = makeColumn(columnName, values);
    FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker maker =
        new FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    return columnBinary;
  }

  private class TestMemorayAllocator implements IMemoryAllocator {
    public final Map<Integer, Boolean> map;

    public TestMemorayAllocator() {
      map = new HashMap<>();
    }

    @Override
    public void setNull(final int index) {
      map.put(index, null);
    }

    @Override
    public void setBoolean(final int index, final boolean value) {
      map.put(index, value);
    }
  }

  public static Stream<Arguments> D_toBinary_1() {
    return Stream.of(
        // columnName, columnValues
        arguments("TEST1", new Boolean[] {true}),
        arguments("TEST1", new Boolean[] {false}),
        arguments("TEST1", new Boolean[] {null, true}),
        arguments("TEST1", new Boolean[] {null, false}),
        arguments("TEST1", new Boolean[] {true, false}),
        arguments("TEST1", new Boolean[] {false, true}),
        arguments("TEST1", new Boolean[] {true, null, false}));
  }

  @ParameterizedTest
  @MethodSource("D_toBinary_1")
  public void T_toBinary_1(final String columnName, final Boolean[] columnValues)
      throws IOException {
    ColumnBinary columnBinary = makeColumnBinary(columnName, columnValues);
    assertEquals(columnName, columnBinary.columnName);
    assertEquals(ColumnType.BOOLEAN, columnBinary.columnType);
    assertEquals(columnValues.length, columnBinary.rowCount);
    assertEquals(
        FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker.class.getName(),
        columnBinary.makerClassName);
  }

  public static Stream<Arguments> D_calcBinarySize_1() {
    return Stream.of(
        // columnName, columnValues
        arguments("TEST1", new Boolean[] {true}, 1),
        arguments("TEST1", new Boolean[] {false}, 1),
        arguments("TEST1", new Boolean[] {null, true}, 2),
        arguments("TEST1", new Boolean[] {null, false}, 2),
        arguments("TEST1", new Boolean[] {true, false}, 2),
        arguments("TEST1", new Boolean[] {false, true}, 2),
        arguments("TEST1", new Boolean[] {true, null, false}, 3));
  }

  @ParameterizedTest
  @MethodSource("D_calcBinarySize_1")
  public void T_calcBinarySize_1(
      final String columnName, final Boolean[] columnValues, final int expected)
      throws IOException {
    IColumn column = makeColumn(columnName, columnValues);
    IColumnAnalizer analizer = new BooleanColumnAnalizer(column);
    IColumnAnalizeResult analizeResult = analizer.analize();
    FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker maker =
        new FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker();
    assertEquals(expected, maker.calcBinarySize(analizeResult));
  }

  public static Stream<Arguments> D_loadInMemoryStorage_1() {
    return Stream.of(
        // columnName, columnValues
        arguments("TEST1", new Boolean[] {true}),
        arguments("TEST1", new Boolean[] {false}),
        arguments("TEST1", new Boolean[] {null, true}),
        arguments("TEST1", new Boolean[] {null, false}),
        arguments("TEST1", new Boolean[] {true, false}),
        arguments("TEST1", new Boolean[] {false, true}),
        arguments("TEST1", new Boolean[] {true, null, false}));
  }

  @ParameterizedTest
  @MethodSource("D_loadInMemoryStorage_1")
  public void T_loadInMemoryStorage_1(final String columnName, final Boolean[] columnValues)
      throws IOException {
    ColumnBinary columnBinary = makeColumnBinary(columnName, columnValues);
    FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker maker =
        new FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker();
    TestMemorayAllocator allocator = new TestMemorayAllocator();
    maker.loadInMemoryStorage(columnBinary, allocator);
    for (int i = 0; i < columnValues.length; i++) {
      assertEquals(columnValues[i], allocator.map.get(i));
    }
  }

  public static Stream<Arguments> D_setBlockIndexNode_1() {
    return Stream.of(
        // columnName, columnValues, expectedHasTrue, expectedHasFalse, expectedHasNull
        arguments("TEST1", new Boolean[] {true}, true, false, false),
        arguments("TEST1", new Boolean[] {false}, false, true, false),
        arguments("TEST1", new Boolean[] {null, true}, true, false, true),
        arguments("TEST1", new Boolean[] {null, false}, false, true, true),
        arguments("TEST1", new Boolean[] {true, false}, true, true, false),
        arguments("TEST1", new Boolean[] {false, true}, true, true, false),
        arguments("TEST1", new Boolean[] {true, null, false}, true, true, true));
  }

  @ParameterizedTest
  @MethodSource("D_setBlockIndexNode_1")
  public void T_setBlockIndexNode_1(
      final String columnName,
      final Boolean[] columnValues,
      final boolean expectedHasTrue,
      final boolean expectedHasFalse,
      final boolean expectedHasNull)
      throws IOException {
    ColumnBinary columnBinary = makeColumnBinary(columnName, columnValues);
    FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker maker =
        new FlagIndexedOptimizedNullArrayDumpBooleanColumnBinaryMaker();
    BlockIndexNode parentNode = new BlockIndexNode();
    maker.setBlockIndexNode(parentNode, columnBinary, 0);
    BlockIndexNode currentNode = parentNode.getChildNode(columnBinary.columnName);
    BooleanBlockIndex blockIndex = (BooleanBlockIndex) currentNode.getBlockIndex();
    assertEquals(expectedHasTrue, blockIndex.hasTrue());
    assertEquals(expectedHasFalse, blockIndex.hasFalse());
    assertEquals(expectedHasNull, blockIndex.hasNull());
  }
}
