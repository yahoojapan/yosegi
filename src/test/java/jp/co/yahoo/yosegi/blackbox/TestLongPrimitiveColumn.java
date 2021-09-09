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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

  public IColumn createTestColumn(final String targetClassName, final long[] longArray) throws IOException {
    return createTestColumn(targetClassName, longArray, null);
  }

  public IColumn createTestColumn( final String targetClassName , final long[] longArray, final int[] loadIndex ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    for ( int i = 0 ; i < longArray.length ; i++ ) {
      column.add( ColumnType.LONG , new LongObj( longArray[i] ) , i );
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    columnBinary.setLoadIndex(loadIndex);
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNotNullColumn(final String targetClassName) throws IOException {
    return createNotNullColumn(targetClassName, null);
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

  public IColumn createNotNullColumn(final String targetClassName, final int[] loadIndex)
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
    columnBinary.setLoadIndex(loadIndex);
    return FindColumnBinaryMaker.get(columnBinary.makerClassName).toColumn(columnBinary);
  }

  public IColumn createNullColumn(final String targetClassName) throws IOException {
    return createNullColumn(targetClassName, null);
  }

  public IColumn createNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    columnBinary.setLoadIndex(loadIndex);
    return FindColumnBinaryMaker.get(columnBinary.makerClassName).toColumn(columnBinary);
  }

  public IColumn createHasNullColumn(final String targetClassName) throws IOException {
    return createHasNullColumn(targetClassName, null);
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

  public IColumn createHasNullColumn(final String targetClassName, final int[] loadIndex)
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
    columnBinary.setLoadIndex(loadIndex);
    return FindColumnBinaryMaker.get(columnBinary.makerClassName).toColumn(columnBinary);
  }

  public IColumn createLastCellColumn(final String targetClassName) throws IOException {
    return createLastCellColumn(targetClassName, null);
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

  public IColumn createLastCellColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.LONG, "column");
    column.add(ColumnType.LONG, new LongObj(lastCellColumnValue(10000)), 10000);

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get(targetClassName);
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode =
        new ColumnBinaryMakerCustomConfigNode("root", defaultConfig);
    ColumnBinary columnBinary =
        maker.toBinary(defaultConfig, null, new CompressResultNode(), column);
    columnBinary.setLoadIndex(loadIndex);
    return FindColumnBinaryMaker.get(columnBinary.makerClassName).toColumn(columnBinary);
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

  public void assertNotNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = createNotNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Long expected = notNullColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getLong());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndex(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {6, 7, 8, 9, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {1, 3, 5, 7, 9};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10, 12, 12, 13, 15};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {6, 6, 7, 8, 9, 10, 10, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {1, 1, 3, 5, 7, 7, 9, 9, 9};
    assertNotNullColumn(targetClassName, loadIndex);
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
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 5, 6, 6, 7, 7, 7, 8, 8};
    IColumn column = createNullColumn(targetClassName, loadIndex);
    // TODO: NullColumn returns 0.
    if (column.getColumnType() == ColumnType.NULL) {
      assertEquals(0, column.size());
    } else {
      assertEquals(loadIndex.length, column.size());
    }
    for (int i = 0; i < loadIndex.length; i++) {
      assertEquals(ColumnType.NULL, column.get(i).getType());
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

  public void assertHasNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = createHasNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Long expected = hasNullColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getLong());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {4, 5, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {1, 3, 5, 7};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 5, 6, 6, 7, 8, 8, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex =
        new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10, 12, 12, 13, 15};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 4, 4, 4};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {4, 4, 5, 6, 6, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {1, 1, 3, 5, 7, 7, 7};
    assertHasNullColumn(targetClassName, loadIndex);
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

  public void assertLastCellColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = createLastCellColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Long expected = lastCellColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getLong());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] loadIndex = new int[lastIndex + 1];
    for (int i = 0; i < loadIndex.length; i++) {
      loadIndex[i] = i;
    }
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10001;
    int[] loadIndex = new int[lastIndex + 1];
    for (int i = 0; i < loadIndex.length; i++) {
      loadIndex[i] = i;
    }
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {9996, 9997, 9998, 9999, 10000};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    List<Integer> loadIndexList = new ArrayList<>();
    int lastIndex = 10000;
    for (int i = 0; i <= lastIndex; i++) {
      int odd = i % 2;
      if (odd == 1) {
        loadIndexList.add(i);
      }
    }
    int[] loadIndex = new int[loadIndexList.size()];
    for (int i = 0; i < loadIndexList.size(); i++) {
      loadIndex[i] = loadIndexList.get(i);
    }
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    List<Integer> loadIndexList = new ArrayList<>();
    int lastIndex = 10000;
    for (int i = 0; i <= lastIndex; i++) {
      int num = 3 - (i % 3);
      for (int j = 0; j < num; j++) {
        loadIndexList.add(i);
      }
    }
    int[] loadIndex = new int[loadIndexList.size()];
    for (int i = 0; i < loadIndexList.size(); i++) {
      loadIndex[i] = loadIndexList.get(i);
    }
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    List<Integer> loadIndexList = new ArrayList<>();
    int lastIndex = 10003;
    for (int i = 0; i <= lastIndex; i++) {
      int num = 3 - (i % 3);
      for (int j = 0; j < num; j++) {
        loadIndexList.add(i);
      }
    }
    int[] loadIndex = new int[loadIndexList.size()];
    for (int i = 0; i < loadIndexList.size(); i++) {
      loadIndex[i] = loadIndexList.get(i);
    }
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 3, 3, 4, 4, 4};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {9996, 9996, 9997, 9998, 9999, 9999, 10000, 10000, 10000};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    List<Integer> loadIndexList = new ArrayList<>();
    int lastIndex = 10000;
    for (int i = 0; i <= lastIndex; i++) {
      int odd = i % 2;
      if (odd == 1) {
        int num = 3 - (i % 3);
        for (int j = 0; j < num; j++) {
          loadIndexList.add(i);
        }
      }
    }
    int[] loadIndex = new int[loadIndexList.size()];
    for (int i = 0; i < loadIndexList.size(); i++) {
      loadIndex[i] = loadIndexList.get(i);
    }
    assertHasNullColumn(targetClassName, loadIndex);
  }

  public void assertTestColumn(final String targetClassName, final long[] longArray, final int[] loadIndexArray) throws IOException {
    IColumn column = createTestColumn(targetClassName, longArray, loadIndexArray);
    if (loadIndexArray == null) {
      assertEquals(column.size(), longArray.length);
      for (int i = 0; i < longArray.length; i++) {
        assertEquals(longArray[i], ((PrimitiveObject) column.get(i).getRow()).getLong());
      }
    } else {
      assertEquals(column.size(), loadIndexArray.length);
      int offset = 0;
      for (int loadIndex : loadIndexArray) {
        assertEquals(longArray[loadIndex], ((PrimitiveObject) column.get(offset).getRow()).getLong());
        offset++;
      }
    }
  }

  public int[] testColumnLoadIndex(final long[] longArray) {
    final int[] loadIndex = new int[longArray.length * 2];
    int index = 0;
    for (int i = 0; i < longArray.length; i++) {
      for (int j = 0; j < 2; j++) {
        loadIndex[index] = i;
        index++;
      }
    }
    return loadIndex;
  }

  public long[] bit0() {
    long[] longArray = new long[10];
    for (int i = 0; i < longArray.length; i++) {
      longArray[i] = 0L;
    }
    return longArray;
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withIntBit0( final String targetClassName ) throws IOException{
    long[] longArray = bit0();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_bit0(final String targetClassName) throws IOException {
    final long[] longArray = bit0();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
  }

  public long[] int1() {
    long[] longArray = new long[] {0L, 0L, 1L, 1L, 0L, 0L, 1L, 1L, 0L, 0L};
    return longArray;
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt1( final String targetClassName ) throws IOException{
    long[] longArray = int1();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int1(final String targetClassName) throws IOException {
    final long[] longArray = int1();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
  }

  public long[] int2() {
    long[] longArray = new long[] {0L, 0L, 1L, 1L, 2L, 2L, 3L, 3L, 0L, 0L};
    return longArray;
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt2( final String targetClassName ) throws IOException{
    long[] longArray = int2();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int2(final String targetClassName) throws IOException {
    final long[] longArray = int2();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
  }

  public long[] int4() {
    long[] longArray = new long[] {0L, 0L, 8L, 8L, 15L, 15L, 1L, 2L, 3L, 4L};
    return longArray;
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt4( final String targetClassName ) throws IOException{
    long[] longArray = int4();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int4(final String targetClassName) throws IOException {
    final long[] longArray = int4();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int8(final String targetClassName) throws IOException {
    final long[] longArray = int8();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt16( final String targetClassName ) throws IOException{
    long[] longArray = int16();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int16(final String targetClassName) throws IOException {
    final long[] longArray = int16();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt24( final String targetClassName ) throws IOException{
    long[] longArray = int24();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int24(final String targetClassName) throws IOException {
    final long[] longArray = int24();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt32( final String targetClassName ) throws IOException{
    long[] longArray = int32();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int32(final String targetClassName) throws IOException {
    final long[] longArray = int32();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt40( final String targetClassName ) throws IOException{
    long[] longArray = int40();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int40(final String targetClassName) throws IOException {
    final long[] longArray = int40();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt48( final String targetClassName ) throws IOException{
    long[] longArray = int48();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int48(final String targetClassName) throws IOException {
    final long[] longArray = int48();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt56( final String targetClassName ) throws IOException{
    long[] longArray = int56();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int56(final String targetClassName) throws IOException {
    final long[] longArray = int56();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
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
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt64( final String targetClassName ) throws IOException{
    long[] longArray = int64();
    assertTestColumn(targetClassName, longArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int64(final String targetClassName) throws IOException {
    final long[] longArray = int64();
    final int[] loadIndex = testColumnLoadIndex(longArray);
    assertTestColumn(targetClassName, longArray, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex(final String targetClassName) {
    int[] loadIndex = new int[] {-1, 0, 1, 2};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column = createNotNullColumn(targetClassName, loadIndex);
        });
  }

  @ParameterizedTest
  @MethodSource("D_longColumnBinaryMaker")
  public void T_load_exception_withLessThanPreviousLoadIndex(final String targetClassName) {
    int[] loadIndex = new int[] {0, 1, 2, 1};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column = createNotNullColumn(targetClassName, loadIndex);
        });
  }
}
