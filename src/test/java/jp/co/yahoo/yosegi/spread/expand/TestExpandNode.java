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
package jp.co.yahoo.yosegi.spread.expand;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class TestExpandNode {

  private ColumnBinary findColumnBinaryFromColumnName( final String columnName , final List<ColumnBinary> columnBinaryList ) {
    for ( ColumnBinary binary : columnBinaryList ) {
      if ( binary.columnName.equals( columnName ) ) {
        return binary;
      }
    }

    return null;
  }

  private List<ColumnBinary> createReaderFromJsonString( final String[] jsonStrings ) throws IOException {
    JacksonMessageReader jsonReader = new JacksonMessageReader();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    YosegiRecordWriter writer = new YosegiRecordWriter(out);
    for ( String json : jsonStrings ) {
      writer.addParserRow( jsonReader.create( json ) );
    }
    writer.close();
    byte[] binary = out.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(binary);
    YosegiReader reader = new YosegiReader();
    reader.setNewStream( in , binary.length , new Configuration() );
    List<ColumnBinary> binaryList = reader.nextRaw();
    reader.close();
    return binaryList;
  }

  @Test
  public void T_expandWithSingleArrayType_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 array<int>
    // | col1 | col2 | col3 |
    // | 0    | 10   | [100,110,120]|
    // | 1    | 11   | [101,]|
    // | 2    | 12   | [102,112]|
    // | 3    | 13   | [103,113,123]|
    // expandSchema: col1 int, col2 int, ex_col3 int
    // rootIndex : [0,0,0,1,2,2,3,3,3]
    // ex_col3 index: none(original)
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":[100,110,120]}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":[101]}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":[102,112]}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":[103,113,123]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertTrue( ( findColumnBinaryFromColumnName( "col1" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col2" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "ex_col3" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col3" , binaryList ) == null ) );

    int[] expandIndex = new int[]{0,0,0,1,2,2,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenEmptyArrayExists_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 array<int>
    // | col1 | col2 | col3 |
    // | 0    | 10   | []|
    // | 1    | 11   | [101]|
    // | 2    | 12   | []|
    // | 3    | 13   | [103,113,123]|
    // expandSchema: col1 int, col2 int, ex_col3 int
    // rootIndex : [1,3,3,3]
    // ex_col3 index: none(original)
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":[]}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":[101]}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":[]}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":[103,113,123]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertTrue( ( findColumnBinaryFromColumnName( "col1" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col2" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "ex_col3" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col3" , binaryList ) == null ) );

    int[] expandIndex = new int[]{1,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenNullExists_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 array<int>
    // | col1 | col2 | col3 |
    // | 0    | 10   | NULL |
    // | 1    | 11   | [101]|
    // | 2    | 12   | NULL |
    // | 3    | 13   | [103,113,123]|
    // expandSchema: col1 int, col2 int, ex_col3 int
    // rootIndex : [1,3,3,3]
    // ex_col3 index: none(original)
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":[101]}" ,
        "{\"col1\":2, \"col2\":12}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":[103,113,123]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertTrue( ( findColumnBinaryFromColumnName( "col1" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col2" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "ex_col3" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col3" , binaryList ) == null ) );

    int[] expandIndex = new int[]{1,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenExpandTargetIsUnionType_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 union<array<int>,string>
    // | col1 | col2 | col3 |
    // | 0    | 10   | "a" |
    // | 1    | 11   | [101]|
    // | 2    | 12   | "b" |
    // | 3    | 13   | [103,113,123]|
    // expandSchema: col1 int, col2 int, ex_col3 int
    // rootIndex : [1,3,3,3]
    // ex_col3 index: none(original)
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":\"a\"}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":[101]}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":\"b\"}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":[103,113,123]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertTrue( ( findColumnBinaryFromColumnName( "col1" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col2" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "ex_col3" , binaryList ) != null ) );
    assertTrue( ( findColumnBinaryFromColumnName( "col3" , binaryList ) == null ) );

    int[] expandIndex = new int[]{1,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ).loadIndex );
    assertEquals( findColumnBinaryFromColumnName( "ex_col3" , binaryList ).columnType , ColumnType.INTEGER );
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenExpandColumnNotExist_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 array<int>
    // | col1 | col2 |
    // | 0    | 10   |
    // | 1    | 11   |
    // | 2    | 12   |
    // | 3    | 13   |
    // expandSchema: col1 int, col2 int
    // rootIndex : []
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10}" ,
        "{\"col1\":1, \"col2\":11}" ,
        "{\"col1\":2, \"col2\":12}" ,
        "{\"col1\":3, \"col2\":13}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ));
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ));
    assertNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ));
    assertNull( findColumnBinaryFromColumnName( "col3" , binaryList ));

    int[] expandIndex = new int[]{};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenChildNodeIsArray_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>> , col3 array<int>
    // | col1 |           col2            | col3 |
    //        |col2-1| col2-2             |         
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11}] | [10001,11001,12001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10}] | [10001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11}] | [10001,11001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11},{"col2-2-1":1,"col2-2-2":11}] | [10001,11001,12001] |
    // expandSchema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>>, ex_col3 int
    // rootIndex : [0,0,0,1,2,2,3,3,3]
    // col2-2 child Index:[0,1,2,3,4,5]
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11},{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001,11001,12001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11},{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001,11001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001,11001,12001]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ));
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ));
    ColumnBinary col2 = findColumnBinaryFromColumnName( "col2" , binaryList );
    ColumnBinary col2_2 = findColumnBinaryFromColumnName( "col2-2" , col2.columnBinaryList );
    ColumnBinary col2_2_inner = col2_2.columnBinaryList.get(0);
    assertNotNull(findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList));
    assertNotNull(findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList));
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ));
    assertNull( findColumnBinaryFromColumnName( "col3" , binaryList ));

    int[] expandIndex = new int[]{0,0,0,1,2,2,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    for ( int i = 0 ; i < expandIndex.length ; i++ ) {
      assertEquals( col2_2.loadIndex[i] , expandIndex[i] );
    }
    int[] childArrayIndex = new int[]{0,1,2,3,4,5};
    ColumnBinary col2_2_1 = findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList);
    ColumnBinary col2_2_2 = findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList);
    assertEquals(col2_2_1.loadIndex.length , childArrayIndex.length);
    assertEquals(col2_2_2.loadIndex.length , childArrayIndex.length);
    for ( int i = 0 ; i < childArrayIndex.length ; i++ ) {
      assertEquals( col2_2_1.loadIndex[i] , childArrayIndex[i] );
      assertEquals( col2_2_2.loadIndex[i] , childArrayIndex[i] );
    }
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenChildNodeIsArrayAndEmptyArrayExists_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>> , col3 array<int>
    // | col1 |           col2            | col3 |
    //        |col2-1| col2-2             |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11}] | [] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10}] | [10001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11}] | [] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11},{"col2-2-1":1,"col2-2-2":11}] | [10001,11001,12001] |
    // expandSchema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>>, ex_col3 int
    // rootIndex : [1,3,3,3]
    // col2-2 child Index:[2,5]
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11},{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11},{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001,11001,12001]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ));
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ));
    ColumnBinary col2 = findColumnBinaryFromColumnName( "col2" , binaryList );
    ColumnBinary col2_2 = findColumnBinaryFromColumnName( "col2-2" , col2.columnBinaryList );
    ColumnBinary col2_2_inner = col2_2.columnBinaryList.get(0);
    assertNotNull(findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList));
    assertNotNull(findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList));
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ));
    assertNull( findColumnBinaryFromColumnName( "col3" , binaryList ));

    int[] expandIndex = new int[]{1,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    for ( int i = 0 ; i < expandIndex.length ; i++ ) {
      assertEquals( col2_2.loadIndex[i] , expandIndex[i] );
    }
    int[] childArrayIndex = new int[]{2,5};
    ColumnBinary col2_2_1 = findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList);
    ColumnBinary col2_2_2 = findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList);
    assertEquals(col2_2_1.loadIndex.length , childArrayIndex.length);
    assertEquals(col2_2_2.loadIndex.length , childArrayIndex.length);
    for ( int i = 0 ; i < childArrayIndex.length ; i++ ) {
      assertEquals( col2_2_1.loadIndex[i] , childArrayIndex[i] );
      assertEquals( col2_2_2.loadIndex[i] , childArrayIndex[i] );
    }
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenChildNodeIsArrayAndChildArrayNodeChildIsUnion_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>> , col3 array<int>
    // | col1 |           col2            | col3 |
    //        |col2-1| col2-2             |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},"a"] | [10001,11001,12001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10}] | [10001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},"b"] | [10001,11001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11},{"col2-2-1":1,"col2-2-2":11}] | [10001,11001,12001] |
    // expandSchema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>>, ex_col3 int
    // rootIndex : [0,0,0,1,2,2,3,3,3]
    // col2-2 child Index:[0,1,2,3,4,5]
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11},\"a\"]},\"col3\":[10001,11001,12001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11},\"b\"]},\"col3\":[10001,11001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001,11001,12001]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ));
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ));
    ColumnBinary col2 = findColumnBinaryFromColumnName( "col2" , binaryList );
    ColumnBinary col2_2 = findColumnBinaryFromColumnName( "col2-2" , col2.columnBinaryList );
    ColumnBinary col2_2_inner = col2_2.columnBinaryList.get(0);
    assertNotNull(findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList));
    assertNotNull(findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList));
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ));
    assertNull( findColumnBinaryFromColumnName( "col3" , binaryList ));

    int[] expandIndex = new int[]{0,0,0,1,2,2,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    for ( int i = 0 ; i < expandIndex.length ; i++ ) {
      assertEquals( col2_2.loadIndex[i] , expandIndex[i] );
    }
    int[] childArrayIndex = new int[]{0,1,2,3,4,5};
    ColumnBinary col2_2_1 = findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList);
    ColumnBinary col2_2_2 = findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList);
    assertEquals(col2_2_1.loadIndex.length , childArrayIndex.length);
    assertEquals(col2_2_2.loadIndex.length , childArrayIndex.length);
    for ( int i = 0 ; i < childArrayIndex.length ; i++ ) {
      assertEquals( col2_2_1.loadIndex[i] , childArrayIndex[i] );
      assertEquals( col2_2_2.loadIndex[i] , childArrayIndex[i] );
    }
  }

  @Test
  public void T_expandWithSingleArrayTypeWhenChildNodeIsArrayAndChildArrayNodeIsUnion_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>> , col3 array<int>
    // | col1 |           col2            | col3 |
    //        |col2-1| col2-2             |
    // | 0    | 11   | "a" | [10001,11001,12001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10}] | [10001] |
    // | 0    | 11   | "b" | [10001,11001] |
    // | 0    | 11   | [{"col2-2-1":0,"col2-2-2":10},{"col2-2-1":1,"col2-2-2":11},{"col2-2-1":1,"col2-2-2":11}] | [10001,11001,12001] |
    // expandSchema: col1 int, col2 spread<col2-1 int, col2-2 array<spread<col2-2-1 int, col2-2-2 int>>>, ex_col3 int
    // rootIndex : [0,0,0,1,2,2,3,3,3]
    // col2-2 child Index:[0,1]
    // expandTarget: col3
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":\"a\"},\"col3\":[10001,11001,12001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":\"b\"},\"col3\":[10001,11001]}" ,
        "{\"col1\":0, \"col2\":{\"col2-1\":11, \"col2-2\":[{\"col2-2-1\":10, \"col2-2-2\":11}]},\"col3\":[10001,11001,12001]}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3"} ) ,
        Arrays.asList( new String[]{"ex_col3"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ));
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ));
    ColumnBinary col2 = findColumnBinaryFromColumnName( "col2" , binaryList );
    ColumnBinary col2_2 = findColumnBinaryFromColumnName( "col2-2" , col2.columnBinaryList );
    ColumnBinary col2_2_string = null;
    ColumnBinary col2_2_array = null;
    for ( ColumnBinary cb : col2_2.columnBinaryList ) {
      if ( cb.columnType == ColumnType.STRING ) {
        col2_2_string = cb;
      } else {
        col2_2_array = cb;
      }
    }
    ColumnBinary col2_2_inner = col2_2_array.columnBinaryList.get(0);
    assertNotNull(findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList));
    assertNotNull(findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList));
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3" , binaryList ));
    assertNull( findColumnBinaryFromColumnName( "col3" , binaryList ));

    int[] expandIndex = new int[]{0,0,0,1,2,2,3,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    for ( int i = 0 ; i < expandIndex.length ; i++ ) {
      assertEquals( col2_2.loadIndex[i] , expandIndex[i] );
      assertEquals( col2_2_string.loadIndex[i] , expandIndex[i] );
      assertEquals( col2_2_array.loadIndex[i] , expandIndex[i] );
    }
    int[] childArrayIndex = new int[]{0,1};
    ColumnBinary col2_2_1 = findColumnBinaryFromColumnName("col2-2-1",col2_2_inner.columnBinaryList);
    ColumnBinary col2_2_2 = findColumnBinaryFromColumnName("col2-2-2",col2_2_inner.columnBinaryList);
    assertEquals(col2_2_1.loadIndex.length , childArrayIndex.length);
    assertEquals(col2_2_2.loadIndex.length , childArrayIndex.length);
    for ( int i = 0 ; i < childArrayIndex.length ; i++ ) {
      assertEquals( col2_2_1.loadIndex[i] , childArrayIndex[i] );
      assertEquals( col2_2_2.loadIndex[i] , childArrayIndex[i] );
    }
  }

  @Test
  public void T_expandWithMultipleArrayType_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 spread<col3-1:array<spread<col3-1-1:array<int>,col3-1-2:array<int>>>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | [100]| [0] |
    //               | [100,110]| [0,1] |
    // | 1    | 11   | [101]| [0,1,2] | 
    //               | [100,110,120]| [0] |
    //               | [100,110]| [0] |
    // | 1    | 12   | [101]| [0,1,2] | 
    //               | [100,110]| [0,1] |
    // | 3    | 13   | [103]| [0,1] |
    //               | [100]| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [0,0,0,1,1,1,1,1,1,2,2,2,3,3]
    // ex_col3-1 index: [0,1,1,2,3,3,3,4,4,5,6,6,7,8]
    // ex_col3-1.col3-1-2 index: [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0,1]}]}}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":[{\"col3-1-1\":[101],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0,1]}]}}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{0,0,0,1,1,1,1,1,1,2,2,2,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{0,1,1,2,3,3,3,4,4,5,6,6,7,8};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{0,1,1,2,3,3,3,4,4,5,6,6,7,8};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithMultipleArrayTypeWithLeafNodeIsEmptyArrayExists_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 spread<col3-1:array<spread<col3-1-1:array<int>,col3-1-2:array<int>>>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | []| [0] |
    //               | [100,110]| [0,1] |
    // | 1    | 11   | []| [0,1,2] |
    //               | [100,110,120]| [0] |
    //               | []| [0] |
    // | 1    | 12   | [101]| [0,1,2] |
    //               | []| [0,1] |
    // | 3    | 13   | [103]| [0,1] |
    //               | []| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [0,0,1,1,1,2,3]
    // ex_col3-1 index: [1,1,3,3,3,5,7]
    // ex_col3-1.col3-1-2 index: [1,2,6,8,9,10,13,14]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":[{\"col3-1-1\":[],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0,1]}]}}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":[{\"col3-1-1\":[101],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[],\"col3-1-2\":[0,1]}]}}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{0,0,1,1,1,2,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{1,1,3,3,3,5,7};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{1,1,3,3,3,5,7};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{1,2,6,8,9,10,13,14};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithMultipleArrayTypeAndEmptyArray_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 spread<col3-1:array<spread<col3-1-1:array<int>,col3-1-2:array<int>>>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | EMPTY |
    // | 1    | 11   | [101]| [0,1,2] |
    //               | [100,110,120]| [0] |
    //               | [100,110]| [0] |
    // | 2    | 12   | EMPTY |
    // | 3    | 13   | [103]| [0,1] |
    //               | [100]| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [1,1,1,1,1,1,3,3]
    // ex_col3-1 index: [0,1,1,1,2,2,3,4]
    // ex_col3-1.col3-1-2 index: [0,1,2,3,4,5,6,7,8,9]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":[]}}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":[]}}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{1,1,1,1,1,1,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{0,1,1,1,2,2,3,4};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{0,1,1,1,2,2,3,4};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{0,1,2,3,4,5,6,7,8,9};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithMultipleArrayTypeAndNullExists_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 spread<col3-1:array<spread<col3-1-1:array<int>,col3-1-2:array<int>>>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | NULL |
    // | 1    | 11   | [101]| [0,1,2] |
    //               | [100,110,120]| [0] |
    //               | [100,110]| [0] |
    // | 2    | 12   | NULL |
    // | 3    | 13   | [103]| [0,1] |
    //               | [100]| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [1,1,1,1,1,1,3,3]
    // ex_col3-1 index: [0,1,1,1,2,2,3,4]
    // ex_col3-1.col3-1-2 index: [0,1,2,3,4,5,6,7,8,9]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":{\"col3-X\":0}}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":{\"col3-X\":0}}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{1,1,1,1,1,1,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{0,1,1,1,2,2,3,4};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{0,1,1,1,2,2,3,4};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{0,1,2,3,4,5,6,7,8,9};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithMultipleArrayTypeAndUnion_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 spread<col3-1:union<array<spread<col3-1-1:array<int>,col3-1-2:array<int>>,int>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | 0 |
    // | 1    | 11   | [101]| [0,1,2] |
    //               | [100,110,120]| [0] |
    //               | [100,110]| [0] |
    // | 2    | 12   | 0 |
    // | 3    | 13   | [103]| [0,1] |
    //               | [100]| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [1,1,1,1,1,1,3,3]
    // ex_col3-1 index: [0,1,1,1,2,2,3,4]
    // ex_col3-1.col3-1-2 index: [0,1,2,3,4,5,6,7,8,9]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":0}}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":0}}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{1,1,1,1,1,1,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{0,1,1,1,2,2,3,4};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{0,1,1,1,2,2,3,4};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{0,1,2,3,4,5,6,7,8,9};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithMultipleArrayTypeAndUnion2_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 union<spread<col3-1:array<spread<col3-1-1:array<int>,col3-1-2:array<int>>>,int>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | 0 |
    // | 1    | 11   | [101]| [0,1,2] |
    //               | [100,110,120]| [0] |
    //               | [100,110]| [0] |
    // | 2    | 12   | 0 |
    // | 3    | 13   | [103]| [0,1] |
    //               | [100]| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [1,1,1,1,1,1,3,3]
    // ex_col3-1 index: [0,1,1,1,2,2,3,4]
    // ex_col3-1.col3-1-2 index: [0,1,2,3,4,5,6,7,8,9]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":0}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":0}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{1,1,1,1,1,1,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{0,1,1,1,2,2,3,4};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{0,1,1,1,2,2,3,4};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{0,1,2,3,4,5,6,7,8,9};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

  @Test
  public void T_expandWithMultipleArrayTypeAndUnion3_equalsExpandArrayIndex() throws IOException {
    // schema: col1 int, col2 int , col3 union<spread<col3-1:array<spread<col3-1-1:array<int>,col3-1-2:array<int>>>,int>>
    // | col1 | col2 | col3 |
    //               | col3-1 |
    //               | col3-1-1 | col3-1-2 |
    // | 0    | 10   | 0 |
    // | 1    | 11   | [101]| [0,1,2] |
    //               | [100,110,120]| [0] |
    //               | [100,110]| [0] |
    // | 2    | 12   | 0 |
    // | 3    | 13   | [103]| [0,1] |
    //               | [100]| [0,1,2] |
    // expandSchema: col1 int, col2 int, ex_col3-1 spread<col3-1-2:array<int>>, ex_col3-1-1 int
    // rootIndex : [1,1,1,1,1,1,3,3]
    // ex_col3-1 index: [0,1,1,1,2,2,3,4]
    // ex_col3-1.col3-1-2 index: [0,1,2,3,4,5,6,7,8,9]
    // ex_col3-1-1 index: none(original)
    // expandTarget: col3.col3-1.col3-1-1
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":0}}" ,
        "{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":[{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]},{\"col3-1-1\":[100,110,120],\"col3-1-2\":[0]},{\"col3-1-1\":[100,110],\"col3-1-2\":[0]}]}}" ,
        "{\"col1\":2, \"col2\":12, \"col3\":0}" ,
        "{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":[{\"col3-1-1\":[103],\"col3-1-2\":[0,1]},{\"col3-1-1\":[100],\"col3-1-2\":[0,1,2]}]}}" } );
    ExpandNode node = new ExpandNode(
        Arrays.asList( new String[]{"col3","col3-1","col3-1-1"} ) ,
        Arrays.asList( new String[]{null,"ex_col3-1","ex_col3-1-1"} )
        );
    node.createExpandColumnBinary( binaryList , new ExpandColumnLink() );
    assertEquals( binaryList.size() , 5 );
    assertNotNull( findColumnBinaryFromColumnName( "col1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1" , binaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ) );

    int[] expandIndex = new int[]{1,1,1,1,1,1,3,3};
    for ( String colName : Arrays.asList( new String[]{"col1","col2"} ) ) {
      ColumnBinary columnBinary = findColumnBinaryFromColumnName( colName , binaryList );
      assertEquals( columnBinary.loadIndex.length , expandIndex.length );
      for ( int i = 0 ; i < expandIndex.length ; i++ ) {
        assertEquals( columnBinary.loadIndex[i] , expandIndex[i] );
      }
    }
    int[] col3_1expandIndex = new int[]{0,1,1,1,2,2,3,4};
    ColumnBinary col3_1 = findColumnBinaryFromColumnName( "ex_col3-1" , binaryList );
    assertEquals( col3_1.loadIndex.length , col3_1expandIndex.length );
    for ( int i = 0 ; i < col3_1expandIndex.length ; i++ ) {
      assertEquals( col3_1.loadIndex[i] , col3_1expandIndex[i] );
    }
    ColumnBinary col3_1_2 = findColumnBinaryFromColumnName( "col3-1-2" , col3_1.columnBinaryList );
    ColumnBinary col3_1_2_inner = col3_1_2.columnBinaryList.get(0);
    int[] col3_1_2expandIndex = new int[]{0,1,1,1,2,2,3,4};
    assertEquals( col3_1_2.loadIndex.length , col3_1_2expandIndex.length );
    for ( int i = 0 ; i < col3_1_2expandIndex.length ; i++ ) {
      assertEquals( col3_1_2.loadIndex[i] , col3_1_2expandIndex[i] );
    }
    int[] col3_1_2_innerExpandIndex = new int[]{0,1,2,3,4,5,6,7,8,9};
    assertEquals( col3_1_2_inner.loadIndex.length , col3_1_2_innerExpandIndex.length );
    for ( int i = 0 ; i < col3_1_2_innerExpandIndex.length ; i++ ) {
      assertEquals( col3_1_2_inner.loadIndex[i] , col3_1_2_innerExpandIndex[i] );
    }
    assertNotNull( findColumnBinaryFromColumnName( "ex_col3-1-1" , binaryList ).loadIndex );
  }

}
