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

public class TestDoublePrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDoubleColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpDoubleColumnBinaryMaker" )
    );
  }

  public static Stream<Arguments> D_doubleColumnBinaryMaker() throws IOException {
    return Stream.of(
            arguments("jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpDoubleColumnBinaryMaker")
    );
  }

  public IColumn createNotNullColumn(final String targetClassName) throws IOException {
    return createNotNullColumn(targetClassName, null);
  }

  public Double notNullColumnValue(int index) {
    final Double[] values =
        new Double[] {
          Double.MAX_VALUE,
          Double.MIN_VALUE,
          -200.0d,
          -300.1d,
          -400.2d,
          -500.3d,
          -600.4d,
          700.5d,
          800.6d,
          900.7d,
          0.0d
        };
    if (index < values.length) {
      return values[index];
    }
    return null;
  }

  public IColumn createNotNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.DOUBLE, "column");
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(0)), 0);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(1)), 1);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(2)), 2);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(3)), 3);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(4)), 4);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(5)), 5);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(6)), 6);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(7)), 7);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(8)), 8);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(9)), 9);
    column.add(ColumnType.DOUBLE, new DoubleObj(notNullColumnValue(10)), 10);

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
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );

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

  public Double hasNullColumnValue(int index) {
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(0, 0d);
            put(4, 4d);
            put(8, 8d);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public IColumn createHasNullColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.DOUBLE, "column");
    column.add(ColumnType.DOUBLE, new DoubleObj(hasNullColumnValue(0)), 0);
    column.add(ColumnType.DOUBLE, new DoubleObj(hasNullColumnValue(4)), 4);
    column.add(ColumnType.DOUBLE, new DoubleObj(hasNullColumnValue(8)), 8);

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

  public Double lastCellColumnValue(int index) {
    final Map<Integer, Double> values =
        new HashMap<Integer, Double>() {
          {
            put(10000, Double.MAX_VALUE);
          }
        };
    if (values.containsKey(index)) {
      return values.get(index);
    }
    return null;
  }

  public IColumn createLastCellColumn(final String targetClassName, final int[] loadIndex)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.DOUBLE, "column");
    column.add(ColumnType.DOUBLE, new DoubleObj(lastCellColumnValue(10000)), 10000);

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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() , Double.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() , Double.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getDouble() , -200.0d );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getDouble() , -300.1d );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getDouble() , -400.2d );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getDouble() , -500.3d );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getDouble() , -600.4d );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getDouble() , 700.5d );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getDouble() , 800.6d );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getDouble() , 900.7d );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getDouble() , 0.0d );
  }

  public void assertNotNullColumn(final String targetClassName, final int[] loadIndex) throws IOException {
    IColumn column = createNotNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Double expected = notNullColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getDouble());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withHead5LoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withTail5LoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {6, 7, 8, 9, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withOddNumberLoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {1, 3, 5, 7, 9};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withAllLoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10, 12, 12, 13, 15};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withHead5LoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withTail5LoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {6, 6, 7, 8, 9, 10, 10, 10};
    assertNotNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadFromNotNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName) throws IOException {
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
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 5, 6, 6, 7, 7, 7, 8, 8};
    IColumn column = createNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    for (int i = 0; i < loadIndex.length; i++) {
      assertEquals(ColumnType.NULL, column.get(i).getType());
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_1( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getDouble() , (double)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getDouble() , (double)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getDouble() , (double)8 );
  }

  public void assertHasNullColumn(final String targetClassName, final int[] loadIndex) throws IOException {
    IColumn column = createHasNullColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Double expected = hasNullColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getDouble());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndex(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndex(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {4, 5, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndex(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {1, 3, 5, 7};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 5, 6, 6, 7, 8, 8, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName) throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 3, 3, 3, 4, 5, 6, 6, 7, 8, 9, 10, 10, 10, 12, 12, 13, 15};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 2, 2, 3, 4, 4, 4, 4};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
          throws IOException {
    int[] loadIndex = new int[] {4, 4, 5, 6, 6, 6, 7, 8};
    assertHasNullColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName) throws IOException {
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
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getDouble() , Double.MAX_VALUE );
  }

  public void assertLastCellColumn(final String targetClassName, final int[] loadIndex) throws IOException {
    IColumn column = createLastCellColumn(targetClassName, loadIndex);
    assertEquals(loadIndex.length, column.size());
    int offset = 0;
    for (int index : loadIndex) {
      Double expected = lastCellColumnValue(index);
      if (expected == null) {
        assertEquals(ColumnType.NULL, column.get(offset).getType());
      } else {
        assertEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getDouble());
      }
      offset++;
    }
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
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
  @MethodSource("D_doubleColumnBinaryMaker")
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
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 1, 2, 3, 4};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {9996, 9997, 9998, 9999, 10000};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndex(final String targetClassName) throws IOException {
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
  @MethodSource("D_doubleColumnBinaryMaker")
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
  @MethodSource("D_doubleColumnBinaryMaker")
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
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {0, 0, 1, 2, 3, 3, 4, 4, 4};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadLastCellColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] loadIndex = new int[] {9996, 9996, 9997, 9998, 9999, 9999, 10000, 10000, 10000};
    assertLastCellColumn(targetClassName, loadIndex);
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_loadLastCellColumn_withOddNumberLoadIndexAndExpand(final String targetClassName) throws IOException {
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

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_load_exception_withLessThan0LoadIndex(final String targetClassName) {
    int[] loadIndex = new int[] {-1, 0, 1, 2};
    assertThrows(
        IOException.class,
        () -> {
          IColumn column = createNotNullColumn(targetClassName, loadIndex);
        });
  }

  @ParameterizedTest
  @MethodSource("D_doubleColumnBinaryMaker")
  public void T_load_exception_withLessThanPreviousLoadIndex(final String targetClassName) {
    int[] loadIndex = new int[] {0, 1, 2, 1};
    assertThrows(
            IOException.class,
            () -> {
              IColumn column = createNotNullColumn(targetClassName, loadIndex);
            });
  }
}
