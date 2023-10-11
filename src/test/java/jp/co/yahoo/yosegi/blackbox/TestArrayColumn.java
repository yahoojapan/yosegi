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
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.inmemory.*;
import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestArrayColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.MaxLengthBasedArrayColumnBinaryMaker" )
    );
  }

  private int getLoadSize(final int[] repetitions) {
    if (repetitions == null) {
      return 0;
    }
    int loadSize = 0;
    for (int size : repetitions) {
      loadSize += size;
    }
    return loadSize;
  }

  private IColumn createArrayColumnFromJsonString(
      final String targetClassName, final String[] jsonStrings) throws IOException {
        return createArrayColumnFromJsonString(targetClassName, jsonStrings, null, 0);
  }

  private IColumn createArrayColumnFromJsonString(
      final String targetClassName,
      final String[] jsonStrings,
      final int[] repetitions,
      final int loadSize)
      throws IOException {
    JacksonMessageReader jsonReader = new JacksonMessageReader();
    ArrayColumn arrayColumn = new ArrayColumn( "test" );
    int addCount = 0;
    for ( String json : jsonStrings ) {
      arrayColumn.add( ColumnType.ARRAY , jsonReader.create( json ) , addCount );
      addCount++;
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , arrayColumn );
    if (repetitions != null) {
      columnBinary.setRepetitions(repetitions, loadSize);
    }

    int loaderSize = (loadSize > 0) ? loadSize : addCount;
    return new YosegiLoaderFactory().create(columnBinary, loaderSize);
  }

  private String[] getJsonStrings() {
    return new String[] {
      "[\"a\",\"b\",\"c\"]",
      "[\"aa\",\"bb\",\"cc\",\"dd\"]",
      "[\"bb\",\"cc\",\"dd\"]",
      "[\"cc\",\"dd\"]",
      "[\"dd\"]"
    };
  }

  private String[] getJsonStringsWithNull() {
    return new String[] {
      "[\"a\",\"b\",\"c\"]",
      "null",
      "null",
      "null",
      "[\"dd\"]"
    };
  }

  private String[] getExpectedValues(final int[] repetitions) {
    String[][] values = {
      {"a", "b", "c"},
      {"aa", "bb", "cc", "dd"},
      {"bb", "cc", "dd"},
      {"cc", "dd"},
      {"dd"}
    };
    List<String> expectedValues = new ArrayList<>();
    for (int i = 0; i < repetitions.length; i++) {
      if (repetitions[i] == 0) {
        continue;
      }
      if (i < values.length) {
        for (int j = 0; j < values[i].length; j++) {
          expectedValues.add(values[i][j]);
        }
      } 
    }
    return expectedValues.toArray(new String[expectedValues.size()]);
  }

  private int getChildLength(final int[] repetitions) {
    String[][] values = {
        {"a", "b", "c"},
        {"aa", "bb", "cc", "dd"},
        {"bb", "cc", "dd"},
        {"cc", "dd"},
        {"dd"}
    };
    boolean isNull = true;
    int childLength = 0;
    for (int i = 0; i < repetitions.length; i++) {
      if (repetitions[i] == 0) {
        continue;
      }
      if (i < values.length) {
        childLength += values[i].length;
        isNull = false;
      }
    }
    // NOTE: If all are null, child.size() is 0 be.
    return isNull ? 0 : childLength;
  }

  private String[][] getExpandValues(final int[] repetitions) {
    String[][] values = {
        {"a", "b", "c"},
        {"aa", "bb", "cc", "dd"},
        {"bb", "cc", "dd"},
        {"cc", "dd"},
        {"dd"}
    };
    List<String[]> expectedValues = new ArrayList<>();
    for (int i = 0; i < repetitions.length; i++) {
      if (repetitions[i] == 0) {
        continue;
      }
      if (i < values.length) {
        for ( int j = 0; j < repetitions[i]; j++ ){
          expectedValues.add(values[i]);
        }
      } else {
        for ( int j = 0; j < repetitions[i]; j++ ){
          expectedValues.add(null);
        }
      }
    }
    return expectedValues.toArray(new String[expectedValues.size()][]);
  }

  private String[][] getExpandValuesWithNull(final int[] repetitions) {
    String[][] values = {
        {"a", "b", "c"},
        null,
        null,
        null,
        {"dd"}
    };
    List<String[]> expectedValues = new ArrayList<>();
    for (int i = 0; i < repetitions.length; i++) {
      if (repetitions[i] == 0) {
        continue;
      }
      if (i < values.length) {
        for ( int j = 0; j < repetitions[i]; j++ ){
          expectedValues.add(values[i]);
        }
      } else {
        for ( int j = 0; j < repetitions[i]; j++ ){
          expectedValues.add(null);
        }
      }
    }
    return expectedValues.toArray(new String[expectedValues.size()][]);
  }

  private void checkExpandArray(final ArrayColumn column, final int[] repetitions) throws IOException {
    String[][] values = getExpandValues(repetitions);
    column.setDefaultCell(NullCell.getInstance());
    assertEquals(column.size(), values.length);
    for (int i = 0; i < column.size(); i++) {
      if (column.get(i).getRow() == null) {
        assertNull(values[i]);
      } else {
        assertNotNull(values[i]);
        List<ICell> columnValue = ((ArrayCell)(column.get(i))).getRow();
        assertEquals(columnValue.size(), values[i].length );
        for (int j = 0; j < columnValue.size(); j++) {
          assertEquals(((PrimitiveObject)(columnValue.get(j).getRow())).getString(), values[i][j]);
        }
      }
    }
  }

  private void checkExpandArrayWithNull(final ArrayColumn column, final int[] repetitions) throws IOException {
    String[][] values = getExpandValuesWithNull(repetitions);
    column.setDefaultCell(NullCell.getInstance());
    assertEquals(column.size(), values.length);
    for (int i = 0; i < column.size(); i++) {
      if (column.get(i).getRow() == null) {
        assertNull(values[i]);
      } else {
        assertNotNull(values[i]);
        List<ICell> columnValue = ((ArrayCell)(column.get(i))).getRow();
        assertEquals(columnValue.size(), values[i].length );
        for (int j = 0; j < columnValue.size(); j++) {
          assertEquals(((PrimitiveObject)(columnValue.get(j).getRow())).getString(), values[i][j]);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_load_childColumnEqualsJsonString( final String targetClassName ) throws IOException{
    IColumn column = createArrayColumnFromJsonString( 
        targetClassName , 
        new String[]{
          "[\"a\",\"b\",\"c\"]" ,
          "[\"aa\",\"bb\",\"cc\",\"dd\"]" ,
          "[\"bb\",\"cc\",\"dd\"]" ,
          "[\"cc\",\"dd\"]" ,
          "[\"dd\"]" } );
    assertEquals( column.getColumnType() , ColumnType.ARRAY );
    assertEquals( column.size() , 5 );

    IColumn child = column.getColumn( 0 );

    assertEquals( child.getColumnType() , ColumnType.STRING );

    assertEquals( child.size() , 13 );

    assertEquals( ( (PrimitiveObject)( child.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( child.get(1).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( child.get(2).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( child.get(3).getRow() ) ).getString() , "aa" );
    assertEquals( ( (PrimitiveObject)( child.get(4).getRow() ) ).getString() , "bb" );
    assertEquals( ( (PrimitiveObject)( child.get(5).getRow() ) ).getString() , "cc" );
    assertEquals( ( (PrimitiveObject)( child.get(6).getRow() ) ).getString() , "dd" );
    assertEquals( ( (PrimitiveObject)( child.get(7).getRow() ) ).getString() , "bb" );
    assertEquals( ( (PrimitiveObject)( child.get(8).getRow() ) ).getString() , "cc" );
    assertEquals( ( (PrimitiveObject)( child.get(9).getRow() ) ).getString() , "dd" );
    assertEquals( ( (PrimitiveObject)( child.get(10).getRow() ) ).getString() , "cc" );
    assertEquals( ( (PrimitiveObject)( child.get(11).getRow() ) ).getString() , "dd" );
    assertEquals( ( (PrimitiveObject)( child.get(12).getRow() ) ).getString() , "dd" );

  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_childColumnEqualsJsonString_withOutOfBoundsLoadSize(
      final String targetClassName) throws IOException {
    int loadSize = 30;
    IColumn column =
        createArrayColumnFromJsonString(
            targetClassName,
            new String[] {
              "[\"a\",\"b\",\"c\"]",
              "[\"aa\",\"bb\",\"cc\",\"dd\"]",
              "[\"bb\",\"cc\",\"dd\"]",
              "[\"cc\",\"dd\"]",
              "[\"dd\"]"
            },
            null,
            loadSize);
    assertEquals(ColumnType.ARRAY, column.getColumnType());
    assertEquals(loadSize, column.size());

    IColumn child = column.getColumn(0);
    assertEquals(ColumnType.STRING, child.getColumnType());
    assertEquals(13, child.size());

    String[] expecteds =
        new String[] {"a", "b", "c", "aa", "bb", "cc", "dd", "bb", "cc", "dd", "cc", "dd", "dd"};
    for (int i = 0; i < expecteds.length; i++) {
      String expected = expecteds[i];
      assertEquals(expected, ((PrimitiveObject) child.get(i).getRow()).getString());
    }
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withAllIndex(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 5
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 13
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["a","b","c","aa","bb","cc","dd","bb","cc","dd","cc","dd","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withAllIndexWithNull(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column = createArrayColumnFromJsonString(
        targetClassName, getJsonStringsWithNull(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 5
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 4
    assertEquals(4, child.size());
    // expected: ["a","b","c","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : new String[]{"a", "b", "c", "dd"}) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArrayWithNull((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withHead2Index(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {1, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 2
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 7
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["a","b","c","aa","bb","cc","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withLast2Index(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 1, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 2
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 3
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["cc","dd","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withOutOfBoundsIndex(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {1, 1, 1, 1, 1, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 6
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 13
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["a","b","c","aa","bb","cc","dd","bb","cc","dd","cc","dd","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withOddNumberIndex(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 1, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 2
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 6
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["aa","bb","cc","dd","cc","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withAllNullIndex(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 1
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.NULL, child.getColumnType());
    // NOTE: If all are null, child.size() is 0 be.
    // childLength: 0
    assertEquals(getChildLength(repetitions), child.size());
    // expected: []
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withAllIndexAndExpand(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 9
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 13
    assertEquals(getChildLength(repetitions), child.size());
    // NOTE: child does not inherit parent's repetitions.
    // expected: ["a","b","c","aa","bb","cc","dd","bb","cc","dd","cc","dd","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withHead2IndexAndExpand(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 3
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 7
    assertEquals(getChildLength(repetitions), child.size());
    // NOTE: child does not inherit parent's repetitions.
    // expected: ["a","b","c","aa","bb","cc","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withLast2IndexAndExpand(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 3, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 4
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 3
    assertEquals(getChildLength(repetitions), child.size());
    // NOTE: child does not inherit parent's repetitions.
    // expected: ["cc","dd","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withOutOfBoundsIndexAndExpand(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {2, 1, 2, 3, 1, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 10
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 13
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["a","b","c","aa","bb","cc","dd","bb","cc","dd","cc","dd","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withOddNumberIndexAndExpand(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {0, 1, 0, 3, 0};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 4
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.STRING, child.getColumnType());
    // childLength: 6
    assertEquals(getChildLength(repetitions), child.size());
    // expected: ["aa","bb","cc","dd","cc","dd"]
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_withAllNullIndexAndExpand(final String targetClassName) throws IOException {
    int[] repetitions = new int[] {0, 0, 0, 0, 0, 2, 1};
    int loadSize = getLoadSize(repetitions);
    IColumn column =
        createArrayColumnFromJsonString(targetClassName, getJsonStrings(), repetitions, loadSize);
    IColumn child = column.getColumn(0);

    assertEquals(ColumnType.ARRAY, column.getColumnType());
    // loadSize: 3
    assertEquals(loadSize, column.size());

    assertEquals(ColumnType.NULL, child.getColumnType());
    // NOTE: If all are null, child.size() is 0 be.
    // childLength: 0
    assertEquals(getChildLength(repetitions), child.size());
    // expected: []
    String[] expectedValues = getExpectedValues(repetitions);
    int index = 0;
    for (String expected : expectedValues) {
      assertEquals(expected, ((PrimitiveObject) child.get(index).getRow()).getString());
      index++;
    }
    checkExpandArray((ArrayColumn)column, repetitions);
  }
}
