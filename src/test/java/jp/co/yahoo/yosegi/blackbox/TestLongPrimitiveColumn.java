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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestLongPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" )
    );
  }

  public static Stream<Arguments> D_longColumnBinaryMaker() {
    return Stream.of(
        arguments("jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker"));
  }

  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    return toColumn(columnBinary, null);
  }

  public IColumn toColumn(final ColumnBinary columnBinary, Integer loadCount) throws IOException {
    if (loadCount == null) {
      loadCount = (columnBinary.isSetLoadSize) ? columnBinary.loadSize : columnBinary.rowCount;
    }
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  public int getLoadSize(final int[] repetitions) {
    if (repetitions == null) {
      return 0;
    }
    int loadSize = 0;
    for (int size : repetitions) {
      loadSize += size;
    }
    return loadSize;
  }

  public IColumn createTestColumn(final String targetClassName, final long[] longArray) throws IOException {
    return createTestColumn(targetClassName, longArray, null, null);
  }

  public IColumn createTestColumn(
      final String targetClassName,
      final long[] longArray,
      final int[] repetitions,
      final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    for ( int i = 0 ; i < longArray.length ; i++ ) {
      column.add( ColumnType.LONG , new LongObj( longArray[i] ) , i );
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public IColumn createNotNullColumn(final String targetClassName) throws IOException {
    return createNotNullColumn(targetClassName, null, null);
  }

  public Long notNullColumnValue(int index) {
    final Long[] values =
        new Long[] {
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          -2000000000L,
          -3000000000L,
          -4000000000L,
          -5000000000L,
          -6000000000L,
          7000000000L,
          8000000000L,
          9000000000L,
          0L
        };
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  public IColumn createNotNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");
    for (int i = 0; i <= 10; i++) {
      column.add(ColumnType.LONG, new LongObj(notNullColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public IColumn createNullColumn(final String targetClassName) throws IOException {
    return createNullColumn(targetClassName, null, null);
  }

  public IColumn createNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public IColumn createHasNullColumn(final String targetClassName) throws IOException {
    return createHasNullColumn(targetClassName, null, null);
  }

  public Long hasNullColumnValue(int index) {
    final Map<Integer, Long> values =
        new HashMap<Integer, Long>() {
          {
            put(0, 0L);
            put(4, 4L);
            put(8, 8L);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public IColumn createHasNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");
    for (int i : new int[] {0, 4, 8}) {
      column.add(ColumnType.LONG, new LongObj(hasNullColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public IColumn createLastCellColumn(final String targetClassName) throws IOException {
    return createLastCellColumn(targetClassName, null, null);
  }

  public Long lastCellColumnValue(int index) {
    final Map<Integer, Long> values =
        new HashMap<Integer, Long>() {
          {
            put(10000, Long.MAX_VALUE);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public IColumn createLastCellColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");
    column.add(ColumnType.LONG, new LongObj(lastCellColumnValue(10000)), 10000);

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  public Long fixedColumnValue(int index) {
    final Long[] values =
        new Long[] {
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE,
            Long.MAX_VALUE
        };
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  public IColumn createfixedColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");
    for (int i = 0; i <= 10; i++) {
      column.add(ColumnType.LONG, new LongObj(fixedColumnValue(i)), i);
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    if (repetitions == null) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions(repetitions, loadSize);
      return toColumn(columnBinary);
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_notNull_1( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() , Long.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() , Long.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getLong() , (long)-2000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getLong() , (long)-3000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getLong() , (long)-4000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getLong() , (long)-5000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getLong() , (long)-6000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getLong() , (long)7000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getLong() , (long)8000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getLong() , (long)9000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getLong() , (long)0 );
  }

  public void assertNotNullColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createNotNullColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      Long expected = notNullColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertEquals(expected.longValue(), ((PrimitiveObject) (column.get(offset).getRow())).getLong());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3, 0, 2, 1, 0, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 3};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 2, 0, 1, 0, 1, 0, 2, 0, 3, 0};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_null_1( final String targetClassName ) throws IOException{
    IColumn column = createNullColumn( targetClassName );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 1, 1, 2, 3, 2};
    IColumn column = createNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
    // TODO: NullColumn returns 0.
    if (column.getColumnType() == ColumnType.NULL) {
      assertEquals(0, column.size());
    } else {
      assertEquals(getLoadSize(repetitions), column.size());
    }
    int offset = 0;
    for (int repetition : repetitions) {
      for (int j = 0; j < repetition; j++) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_1( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() , (long)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getLong() , (long)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getLong() , (long)8 );
  }

  public void assertHasNullColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createHasNullColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      Long expected = hasNullColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertEquals(expected.longValue(), ((PrimitiveObject) (column.get(offset).getRow())).getLong());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0, 1, 0, 1, 0};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 1, 1, 2, 1, 3};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3, 0, 2, 1, 0, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 4};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 2, 1, 3, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 2, 0, 1, 0, 1, 0, 3, 0};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getLong() , Long.MAX_VALUE );
  }

  public void assertLastCellColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createLastCellColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      Long expected = lastCellColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertEquals(expected.longValue(), ((PrimitiveObject) (column.get(offset).getRow())).getLong());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    Arrays.fill(repetitions, 1);
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10001;
    int[] repetitions = new int[lastIndex + 1];
    Arrays.fill(repetitions, 1);
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = ((lastIndex - i) < 5) ? 1 : 0;
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = i % 2;
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = 3 - (i % 3);
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int lastIndex = 10003;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = 3 - (i % 3);
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 1, 2, 3};
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    final Map<Integer, Integer> values =
        new HashMap<Integer, Integer>() {
          {
            put(9996, 2);
            put(9997, 1);
            put(9998, 1);
            put(9999, 2);
            put(10000, 3);
          }
        };
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      repetitions[i] = values.getOrDefault(i, 0);
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    for (int i = 0; i < repetitions.length; i++) {
      int odd = i % 2;
      repetitions[i] = (odd == 1) ? 3 - (i % 3) : 0;
    }
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  public void assertTestColumn(final String targetClassName, final long[] longArray)
      throws IOException {
    assertTestColumn(targetClassName, longArray, null, null);
  }

  public void assertTestColumn(
      final String targetClassName,
      final long[] longArray,
      final int[] repetitions,
      final Integer loadSize)
      throws IOException {
    IColumn column = createTestColumn(targetClassName, longArray, repetitions, loadSize);
    if (repetitions == null) {
      assertEquals(longArray.length, column.size());
      for (int i = 0; i < longArray.length; i++) {
        assertEquals(longArray[i], ((PrimitiveObject) column.get(i).getRow()).getLong());
      }
    } else {
      assertEquals(loadSize.intValue(), column.size());
      int offset = 0;
      for (int i = 0; i < repetitions.length; i++) {
        for (int j = 0; j < repetitions[i]; j++) {
          assertEquals(longArray[i], ((PrimitiveObject) column.get(offset).getRow()).getLong());
          offset++;
        }
      }
    }
  }

  public int[] testColumnRepetitions(final long[] longArray) {
    final int[] repetitions = new int[longArray.length];
    for (int i = 0; i < longArray.length; i++) {
      repetitions[i] = 2;
    }
    return repetitions;
  }

  public long[] bit0() {
    long[] longArray = new long[10];
    for (int i = 0; i < longArray.length; i++) {
      longArray[i] = 0L;
    }
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withIntBit0(final String targetClassName)
      throws IOException {
    long[] longArray = bit0();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_bit0(final String targetClassName)
      throws IOException {
    final long[] longArray = bit0();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int1() {
    long[] longArray = new long[] {0L, 0L, 1L, 1L, 0L, 0L, 1L, 1L, 0L, 0L};
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt1(final String targetClassName)
      throws IOException {
    long[] longArray = int1();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int1(final String targetClassName)
      throws IOException {
    final long[] longArray = int1();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int2() {
    long[] longArray = new long[] {0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 0L, 0L};
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt2(final String targetClassName)
      throws IOException {
    long[] longArray = int2();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int2(final String targetClassName)
      throws IOException {
    final long[] longArray = int2();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int4() {
    long[] longArray = new long[] {0L, 0L, 8L, 8L, 15L, 15L, 1L, 2L, 3L, 4L};
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt4(final String targetClassName)
      throws IOException {
    long[] longArray = int4();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int4(final String targetClassName)
      throws IOException {
    final long[] longArray = int4();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int8() {
    long[] longArray =
        new long[] {
          (long) Byte.MAX_VALUE, (long) Byte.MIN_VALUE, 0L, 0L, 64L, -64L, 32L, -32L, 16L, -16L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt8( final String targetClassName ) throws IOException{
    long[] longArray = int8();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int8(final String targetClassName)
      throws IOException {
    final long[] longArray = int8();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int16() {
    long[] longArray =
        new long[] {
          (long) Short.MAX_VALUE,
          (long) Short.MIN_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MIN_VALUE,
          1L,
          1L,
          -1L,
          -1L,
          2L,
          2L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt16(final String targetClassName)
      throws IOException {
    long[] longArray = int16();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int16(final String targetClassName)
      throws IOException {
    final long[] longArray = int16();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int24() {
    long max = 0xFFFFFFL;
    long[] longArray =
        new long[] {
          (long) Short.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          max,
          max,
          1L,
          1L,
          2L,
          2L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt24(final String targetClassName)
      throws IOException {
    long[] longArray = int24();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int24(final String targetClassName)
      throws IOException {
    final long[] longArray = int24();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int32() {
    long[] longArray =
        new long[] {
          (long) Integer.MAX_VALUE,
          (long) Integer.MIN_VALUE,
          (long) Short.MAX_VALUE,
          (long) Short.MIN_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MIN_VALUE,
          1L,
          1L,
          2L,
          2L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt32(final String targetClassName)
      throws IOException {
    long[] longArray = int32();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int32(final String targetClassName)
      throws IOException {
    final long[] longArray = int32();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int40() {
    long max40 = 0xFFFFFFFFFFL;
    long[] longArray =
        new long[] {
          (long) Integer.MAX_VALUE,
          (long) Integer.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          0L,
          0L,
          max40,
          max40,
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt40(final String targetClassName)
      throws IOException {
    long[] longArray = int40();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int40(final String targetClassName)
      throws IOException {
    final long[] longArray = int40();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int48() {
    long max48 = 0xFFFFFFFFFFL;
    long[] longArray =
        new long[] {
          (long) Integer.MAX_VALUE,
          (long) Integer.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          max48,
          max48,
          0L,
          0L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt48(final String targetClassName)
      throws IOException {
    long[] longArray = int48();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int48(final String targetClassName)
      throws IOException {
    final long[] longArray = int48();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int56() {
    long max56 = 0xFFFFFFFFFFFFL;
    long[] longArray =
        new long[] {
          (long) Integer.MAX_VALUE,
          (long) Integer.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Short.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MAX_VALUE,
          max56,
          max56,
          0L,
          0L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt56(final String targetClassName)
      throws IOException {
    long[] longArray = int56();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int56(final String targetClassName)
      throws IOException {
    final long[] longArray = int56();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  public long[] int64() {
    long[] longArray =
        new long[] {
          (long) Long.MAX_VALUE,
          (long) Long.MIN_VALUE,
          (long) Integer.MAX_VALUE,
          (long) Integer.MIN_VALUE,
          (long) Short.MAX_VALUE,
          (long) Short.MIN_VALUE,
          (long) Byte.MAX_VALUE,
          (long) Byte.MIN_VALUE,
          0L,
          0L
        };
    return longArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt64(final String targetClassName)
      throws IOException {
    long[] longArray = int64();
    assertTestColumn(targetClassName, longArray);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int64(final String targetClassName)
      throws IOException {
    final long[] longArray = int64();
    final int[] repetitions = testColumnRepetitions(longArray);
    assertTestColumn(targetClassName, longArray, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex(final String targetClassName) {
    int[] repetitions = new int[] {-1, 0, 1, 2};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
        });
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex_withOutOfBoundsLoadIndexAndExpand(
      final String targetClassName) {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
        });
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadFixedColumn_exception_withLessThan0LoadIndex(
      final String targetClassName) {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createfixedColumn(targetClassName, repetitions, getLoadSize(repetitions));
        });
  }
}
