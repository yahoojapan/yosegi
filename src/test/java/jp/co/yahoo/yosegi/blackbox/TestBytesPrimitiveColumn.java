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

public class TestBytesPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker" )
    );
  }

  public static Stream<Arguments> D_bytesColumnBinaryMaker() throws IOException {
    return Stream.of(
        arguments("jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker"));
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

  public IColumn createNotNullColumn(final String targetClassName) throws IOException {
    return createNotNullColumn(targetClassName, null, null);
  }

  public byte[] notNullColumnValue(int index) {
    final String[] values =
        new String[] {"a", "ab", "abc", "abcd", "b", "bc", "bcd", "bcde", "c", "cd", ""};
    if (index < values.length) {
      return values[index].getBytes();
    }
    return null;
  }

  public IColumn createNotNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTES, "column");
    for (int i = 0; i <= 10; i++) {
      column.add(ColumnType.BYTES, new BytesObj(notNullColumnValue(i)), i);
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
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "column" );

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

  public IColumn createHasNullColumn(final String targetClassName) throws IOException {
    return createHasNullColumn(targetClassName, null, null);
  }

  public byte[] hasNullColumnValue(int index) {
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(0, "a");
            put(4, "b");
            put(8, "c");
          }
        };
    if (values.containsKey(index)) {
      return values.get(index).getBytes();
    }
    return null;
  }

  public IColumn createHasNullColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTES, "column");
    for (int i : new int[] {0, 4, 8}) {
      column.add(ColumnType.BYTES, new BytesObj(hasNullColumnValue(i)), i);
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

  public byte[] lastCellColumnValue(int index) {
    final Map<Integer, String> values =
        new HashMap<Integer, String>() {
          {
            put(10000, "c");
          }
        };
    if (values.containsKey(index)) {
      return values.get(index).getBytes();
    }
    return null;
  }

  public IColumn createLastCellColumn(
      final String targetClassName, final int[] repetitions, final Integer loadSize)
      throws IOException {
    IColumn column = new PrimitiveColumn(ColumnType.BYTES, "column");
    for (int i : new int[] {10000}) {
      column.add(ColumnType.BYTES, new BytesObj(lastCellColumnValue(i)), i);
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
  public void T_encodeAndDecode_equalsSetValue_withNotNull( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "" );
  }

  public void assertNotNullColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createNotNullColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      byte[] expected = notNullColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertArrayEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getBytes());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3, 0, 2, 1, 0, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 0, 2, 1, 1, 1, 3};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNotNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 2, 0, 1, 0, 1, 0, 2, 0, 3, 0};
    assertNotNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_encodeAndDecode_equalsSetValue_withNull(final String targetClassName)
      throws IOException {
    IColumn column = createNullColumn(targetClassName);
    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 1, 1, 2, 3, 2};
    IColumn column = createNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
    assertEquals(getLoadSize(repetitions), column.size());
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
  public void T_encodeAndDecode_equalsSetValue_withHasNull( final String targetClassName ) throws IOException{
    // NOTE: Spread size must be passed because correct load size can't be gotten.
    IColumn column = createHasNullColumn(targetClassName, null, 9);
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
  }

  public void assertHasNullColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createHasNullColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      byte[] expected = hasNullColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertArrayEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getBytes());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 1, 1, 1, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0, 1, 0, 1, 0};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withAllLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 1, 1, 2, 1, 3};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withOutOfBoundsLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1, 2, 1, 1, 1, 3, 0, 2, 1, 0, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 3, 1, 4};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withTail5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 2, 1, 3, 1, 1};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadHasNullColumn_withOddNumberLoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {0, 2, 0, 1, 0, 1, 0, 3, 0};
    assertHasNullColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_lastCellOnly( final String targetClassName ) throws IOException{
    // NOTE: Spread size must be passed because correct load size can't be gotten.
    IColumn column = createLastCellColumn(targetClassName, null, 10001);
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getString() , "c" );
  }

  public void assertLastCellColumn(
      final String targetClassName, final int[] repetitions, final int loadSize)
      throws IOException {
    IColumn column = createLastCellColumn(targetClassName, repetitions, loadSize);
    assertEquals(loadSize, column.size());
    int offset = 0;
    for (int i = 0; i < repetitions.length; i++) {
      byte[] expected = lastCellColumnValue(i);
      for (int j = 0; j < repetitions[i]; j++) {
        if (expected == null) {
          assertEquals(ColumnType.NULL, column.get(offset).getType());
        } else {
          assertArrayEquals(expected, ((PrimitiveObject) (column.get(offset).getRow())).getBytes());
        }
        offset++;
      }
    }
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadLastCellColumn_withAllLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10000;
    int[] repetitions = new int[lastIndex + 1];
    Arrays.fill(repetitions, 1);
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadLastCellColumn_withOutOfBoundsLoadIndex(final String targetClassName)
      throws IOException {
    int lastIndex = 10001;
    int[] repetitions = new int[lastIndex + 1];
    Arrays.fill(repetitions, 1);
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndex(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
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
  @MethodSource("D_bytesColumnBinaryMaker")
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
  @MethodSource("D_bytesColumnBinaryMaker")
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
  @MethodSource("D_bytesColumnBinaryMaker")
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
  @MethodSource("D_bytesColumnBinaryMaker")
  public void T_loadLastCellColumn_withHead5LoadIndexAndExpand(final String targetClassName)
      throws IOException {
    int[] repetitions = new int[] {2, 1, 1, 2, 3};
    assertLastCellColumn(targetClassName, repetitions, getLoadSize(repetitions));
  }

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
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
  @MethodSource("D_bytesColumnBinaryMaker")
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

  @ParameterizedTest
  @MethodSource("D_bytesColumnBinaryMaker")
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
  @MethodSource("D_bytesColumnBinaryMaker")
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
}
