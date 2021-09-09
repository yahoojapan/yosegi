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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestBytePrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" )
    );
  }

  public static Stream<Arguments> D_byteColumnBinaryMaker() {
    return Stream.of(
        arguments("jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker"),
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker"));
  }

  public IColumn createTestColumn(final String targetClassName, final byte[] valueArray)
      throws IOException {
    return createTestColumn(targetClassName, valueArray, null);
  }

  public IColumn createTestColumn( final String targetClassName , final byte[] valueArray, final int[] loadIndex ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    for ( int i = 0 ; i < valueArray.length ; i++ ) {
      column.add( ColumnType.BYTE , new ByteObj( valueArray[i] ) , i );
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

  public Byte notNullColumnValue(int index) {
    final Byte[] values =
        new Byte[] {
          Byte.MAX_VALUE,
          Byte.MIN_VALUE,
          (byte) -2,
          (byte) -3,
          (byte) -4,
          (byte) -5,
          (byte) -6,
          (byte) 7,
          (byte) 8,
          (byte) 9,
          (byte) 0
        };
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  public IColumn createNotNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTE, "column");
    for (int i = 0; i <= 10; i++) {
      column.add(ColumnType.BYTE, new ByteObj(notNullColumnValue(i)), i);
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

  public IColumn createNullColumn( final String targetClassName, final int[] loadIndex ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    columnBinary.setLoadIndex(loadIndex);
    return  FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createHasNullColumn(final String targetClassName) throws IOException {
    return createHasNullColumn(targetClassName, null);
  }

  public Byte hasNullColumnValue(int index) {
    final Map<Integer, Byte> values =
        new HashMap<Integer, Byte>() {
          {
            put(0, (byte) 0);
            put(4, (byte) 4);
            put(8, (byte) 8);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public IColumn createHasNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTE, "column");
    for (int i : new int[] {0, 4, 8}) {
      column.add(ColumnType.BYTE, new ByteObj(hasNullColumnValue(i)), i);
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

  public Byte lastCellColumnValue(int index) {
    final Map<Integer, Byte> values =
        new HashMap<Integer, Byte>() {
          {
            put(10000, Byte.MAX_VALUE);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public IColumn createLastCellColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTE, "column");
    for (int i : new int[] {10000}) {
      column.add(ColumnType.BYTE, new ByteObj(lastCellColumnValue(i)), i);
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

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_notNull_1( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() , Byte.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte() , Byte.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte() , (byte)-2 );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte() , (byte)-3 );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte() , (byte)-4 );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getByte() , (byte)-5 );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getByte() , (byte)-6 );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getByte() , (byte)7 );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getByte() , (byte)8 );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getByte() , (byte)9 );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getByte() , (byte)0 );
  }

  public void assertNotNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = createNotNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Byte expected = notNullColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getByte());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {6, 7, 8, 9, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {1, 3, 5, 7, 9};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex =
        new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10, 12, 12, 13, 15};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {6, 6, 7, 8, 9, 10, 10, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
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
  @MethodSource("D_byteColumnBinaryMaker")
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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte() , (byte)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte() , (byte)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getByte() , (byte)8 );
  }

  public void assertHasNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = createHasNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Byte expected = hasNullColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getByte());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {4, 5, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {1, 3, 5, 7};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 5, 6, 6, 7, 8, 8, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex =
        new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10, 12, 12, 13, 15};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 4, 4, 4};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {4, 4, 5, 6, 6, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
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
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getByte() , Byte.MAX_VALUE );
  }

  public void assertLastCellColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = createLastCellColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Byte expected = lastCellColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getByte());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
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
  @MethodSource("D_byteColumnBinaryMaker")
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
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {9996, 9997, 9998, 9999, 10000};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
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
  @MethodSource("D_byteColumnBinaryMaker")
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
  @MethodSource("D_byteColumnBinaryMaker")
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
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 3, 3, 4, 4, 4};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {9996, 9996, 9997, 9998, 9999, 9999, 10000, 10000, 10000};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
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

  public void assertTestColumn(
      final String targetClassName, final byte[] valueArray, final int[] loadIndexArray)
      throws IOException {
    IColumn column = createTestColumn(targetClassName, valueArray, loadIndexArray);
    if (loadIndexArray == null) {
      assertEquals(column.size(), valueArray.length);
      for (int i = 0; i < valueArray.length; i++) {
        assertEquals(valueArray[i], ((PrimitiveObject) column.get(i).getRow()).getByte());
      }
    } else {
      assertEquals(column.size(), loadIndexArray.length);
      int offset = 0;
      for (int loadIndex : loadIndexArray) {
        assertEquals(
            valueArray[loadIndex], ((PrimitiveObject) column.get(offset).getRow()).getByte());
        offset++;
      }
    }
  }

  public int[] testColumnLoadIndex(final byte[] valueArray) {
    final int[] loadIndex = new int[valueArray.length * 2];
    int index = 0;
    for (int i = 0; i < valueArray.length; i++) {
      for (int j = 0; j < 2; j++) {
        loadIndex[index] = i;
        index++;
      }
    }
    return loadIndex;
  }

  public byte[] bit0() {
    byte[] valueArray = new byte[10];
    for (int i = 0; i < valueArray.length; i++) {
      valueArray[i] = (byte) 0;
    }
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withIntBit0(final String targetClassName)
      throws IOException {
    byte[] valueArray = bit0();
    assertTestColumn(targetClassName, valueArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_bit0(final String targetClassName)
      throws IOException {
    final byte[] valueArray = bit0();
    final int[] loadIndex = testColumnLoadIndex(valueArray);
    assertTestColumn(targetClassName, valueArray, loadIndex);
  }

  public byte[] int1() {
    byte[] valueArray =
        new byte[] {
          (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 0,
          (byte) 0
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt1(final String targetClassName)
      throws IOException {
    byte[] valueArray = int1();
    assertTestColumn(targetClassName, valueArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int1(final String targetClassName)
      throws IOException {
    final byte[] valueArray = int1();
    final int[] loadIndex = testColumnLoadIndex(valueArray);
    assertTestColumn(targetClassName, valueArray, loadIndex);
  }

  public byte[] int2() {
    byte[] valueArray =
        new byte[] {
          (byte) 0, (byte) 0, (byte) 1, (byte) 1, (byte) 2, (byte) 2, (byte) 3, (byte) 3, (byte) 0,
          (byte) 0
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt2(final String targetClassName)
      throws IOException {
    byte[] valueArray = int2();
    assertTestColumn(targetClassName, valueArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int2(final String targetClassName)
      throws IOException {
    final byte[] valueArray = int2();
    final int[] loadIndex = testColumnLoadIndex(valueArray);
    assertTestColumn(targetClassName, valueArray, loadIndex);
  }

  public byte[] int4() {
    byte[] valueArray =
        new byte[] {
          (byte) 0, (byte) 0, (byte) 8, (byte) 8, (byte) 15, (byte) 15, (byte) 1, (byte) 2,
          (byte) 3, (byte) 4
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withInt4(final String targetClassName)
      throws IOException {
    byte[] valueArray = int4();
    assertTestColumn(targetClassName, valueArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int4(final String targetClassName)
      throws IOException {
    final byte[] valueArray = int4();
    final int[] loadIndex = testColumnLoadIndex(valueArray);
    assertTestColumn(targetClassName, valueArray, loadIndex);
  }

  public byte[] int8() {
    byte[] valueArray =
        new byte[] {
          Byte.MAX_VALUE,
          Byte.MIN_VALUE,
          (byte) 0,
          (byte) 0,
          (byte) 64,
          (byte) -64,
          (byte) 32,
          (byte) -32,
          (byte) 16,
          (byte) -16
        };
    return valueArray;
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt8( final String targetClassName ) throws IOException{
    byte[] valueArray = int8();
    assertTestColumn(targetClassName, valueArray, null);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_loadTestColumn_withAllLoadIndexAndExpand_int8(final String targetClassName)
      throws IOException {
    final byte[] valueArray = int8();
    final int[] loadIndex = testColumnLoadIndex(valueArray);
    assertTestColumn(targetClassName, valueArray, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex(final String targetClassName) {
    int[] loadIndex = new int[] {-1, 0, 1, 2};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column = createNotNullColumn(targetClassName, loadIndex);
        });
  }

  @ParameterizedTest
  @MethodSource("D_byteColumnBinaryMaker")
  public void T_load_exception_withLessThanPreviousLoadIndex(final String targetClassName) {
    int[] loadIndex = new int[] {0, 1, 2, 1};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column = createNotNullColumn(targetClassName, loadIndex);
        });
  }
}
