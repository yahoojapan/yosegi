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
package jp.co.yahoo.yosegi.spread.flatten;

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

public class TestFlattenFunction {

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
  public void T_flatten_equals_whenPrimitiveTypeOnlyAndSameLinkColumnName() throws IOException {
    // schema: col1 int, col2 int , col3 int
    // | col1 | col2 | col3 |
    // | 0    | 10   | 100 |
    // | 1    | 11   | 101 |
    // | 2    | 12   | 102 |
    // | 3    | 13   | 103 |
    // flattenSchema: col2 int, col3 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":100}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":101}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":102}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":103}" } );
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "col2" , new String[]{"col2"} ) );
    flatten.add( new FlattenColumn( "col3" , new String[]{"col3"} ) );

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 2 );
    assertNull( findColumnBinaryFromColumnName( "col1" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col3" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenPrimitiveTypeOnlyAndSameLinkColumnNameAndFilter() throws IOException {
    // schema: col1 int, col2 int , col3 int
    // | col1 | col2 | col3 |
    // | 0    | 10   | 100 |
    // | 1    | 11   | 101 |
    // | 2    | 12   | 102 |
    // | 3    | 13   | 103 |
    // flattenSchema: col2 int, col3 int
    // flteredFlattenSchema: col2 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":100}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":101}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":102}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":103}" } );
    FlattenFunction flatten = new FlattenFunction();
    String[] col2OriginalNames = new String[]{"col2"};
    String[] col3OriginalNames = new String[]{"col3"};
    flatten.add( new FlattenColumn( "col2" , col2OriginalNames ) );
    flatten.add( new FlattenColumn( "col3" , col3OriginalNames ) );

    String[] newCol2OriginalName = flatten.getFlattenColumnName( "col2" );
    assertEquals( col2OriginalNames.length , newCol2OriginalName.length );
    for ( int i = 0; i < col2OriginalNames.length; i++ ) {
      assertEquals( col2OriginalNames[i] , newCol2OriginalName[i] );
    }

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 1 );
    assertNull( findColumnBinaryFromColumnName( "col1" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "col3" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenPrimitiveTypeOnly() throws IOException {
    // schema: col1 int, col2 int , col3 int
    // | col1 | col2 | col3 |
    // | 0    | 10   | 100 |
    // | 1    | 11   | 101 |
    // | 2    | 12   | 102 |
    // | 3    | 13   | 103 |
    // flattenSchema: f_col2 int, f_col3 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":100}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":101}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":102}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":103}" } );
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "f_col2" , new String[]{"col2"} ) );
    flatten.add( new FlattenColumn( "f_col3" , new String[]{"col3"} ) );

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 2 );
    assertNull( findColumnBinaryFromColumnName( "col1" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "col3" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenPrimitiveTypeOnlyAndFilter() throws IOException {
    // schema: col1 int, col2 int , col3 int
    // | col1 | col2 | col3 |
    // | 0    | 10   | 100 |
    // | 1    | 11   | 101 |
    // | 2    | 12   | 102 |
    // | 3    | 13   | 103 |
    // flattenSchema: f_col2 int, f_col3 int
    // flteredFlattenSchema: f_col2 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col1\":0, \"col2\":10, \"col3\":100}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":101}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":102}" ,
        "{\"col1\":0, \"col2\":10, \"col3\":103}" } );
    FlattenFunction flatten = new FlattenFunction();
    String[] col2OriginalNames = new String[]{"col2"};
    String[] col3OriginalNames = new String[]{"col3"};
    flatten.add( new FlattenColumn( "f_col2" , col2OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3" , col3OriginalNames ) );

    String[] newCol2OriginalName = flatten.getFlattenColumnName( "f_col2" );
    assertEquals( col2OriginalNames.length , newCol2OriginalName.length );
    for ( int i = 0; i < col2OriginalNames.length; i++ ) {
      assertEquals( col2OriginalNames[i] , newCol2OriginalName[i] );
    }

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 1 );
    assertNull( findColumnBinaryFromColumnName( "col1" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "col3" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "f_col3" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenSpreadType() throws IOException {
    // schema: col spread<col1 int, col2 int , col3 spread<col3-1 int, col3-2 int>>
    // |        col         |
    // | col1 | col2 | col3 |
    //               | col3-1 | col3-2 |
    // | 0    | 10   | 100 | 1000 |
    // | 1    | 11   | 101 | 1001 |
    // | 2    | 12   | 102 | 1002 |
    // | 3    | 13   | 103 | 1003 |
    // flattenSchema: f_col2 int , f_col3-1 int, f_col3-2 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col\":{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":100,\"col3-2\":1000}}}" ,
        "{\"col\":{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":101,\"col3-2\":1001}}}" ,
        "{\"col\":{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":102,\"col3-2\":1002}}}" ,
        "{\"col\":{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":103,\"col3-2\":1003}}}" } );
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "f_col2" , new String[]{"col","col2"} ) );
    flatten.add( new FlattenColumn( "f_col3-1" , new String[]{"col","col3","col3-1"} ) );
    flatten.add( new FlattenColumn( "f_col3-2" , new String[]{"col","col3","col3-2"} ) );

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 3 );
    assertNull( findColumnBinaryFromColumnName( "col" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3-1" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3-2" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenSpreadTypeAndFilter() throws IOException {
    // schema: col spread<col1 int, col2 int , col3 spread<col3-1 int, col3-2 int>>
    // |        col         |
    // | col1 | col2 | col3 |
    //               | col3-1 | col3-2 |
    // | 0    | 10   | 100 | 1000 |
    // | 1    | 11   | 101 | 1001 |
    // | 2    | 12   | 102 | 1002 |
    // | 3    | 13   | 103 | 1003 |
    // flattenSchema: f_col2 int , f_col3-1 int, f_col3-2 int
    // flteredFlattenSchema: f_col2 int, f_col3-2 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col\":{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":100,\"col3-2\":1000}}}" ,
        "{\"col\":{\"col1\":1, \"col2\":11, \"col3\":{\"col3-1\":101,\"col3-2\":1001}}}" ,
        "{\"col\":{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":102,\"col3-2\":1002}}}" ,
        "{\"col\":{\"col1\":3, \"col2\":13, \"col3\":{\"col3-1\":103,\"col3-2\":1003}}}" } );
    String[] col2OriginalNames = new String[]{"col","col2"};
    String[] col3_1OriginalNames = new String[]{"col","col3","col3-1"};
    String[] col3_2OriginalNames = new String[]{"col","col3","col3-2"};
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "f_col2" , col2OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3-1" , col3_1OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3-2" , col3_2OriginalNames ) );
    String[] newCol2OriginalName = flatten.getFlattenColumnName( "f_col2" );
    assertEquals( col2OriginalNames.length , newCol2OriginalName.length );
    for ( int i = 0; i < col2OriginalNames.length; i++ ) {
      assertEquals( col2OriginalNames[i] , newCol2OriginalName[i] );
    }
    String[] newCol3_2OriginalName = flatten.getFlattenColumnName( "f_col3-2" );
    assertEquals( col3_2OriginalNames.length , newCol3_2OriginalName.length );
    for ( int i = 0; i < col3_2OriginalNames.length; i++ ) {
      assertEquals( col3_2OriginalNames[i] , newCol3_2OriginalName[i] );
    }

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 2 );
    assertNull( findColumnBinaryFromColumnName( "col" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "f_col3-1" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3-2" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenUnionSpreadType() throws IOException {
    // schema: col spread<col1 int, col2 int , col3 union<spread<col3-1 int, col3-2 int>,int>>
    // |        col         |
    // | col1 | col2 | col3 |
    //               | col3-1 | col3-2 |
    // | 0    | 10   | 100 | 1000 |
    // | 1    | 11   |    1000    |
    // | 2    | 12   | 102 | 1002 |
    // | 3    | 13   |    1003    |
    // flattenSchema: f_col2 int , f_col3-1 int, f_col3-2 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col\":{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":100,\"col3-2\":1000}}}" ,
        "{\"col\":{\"col1\":1, \"col2\":11, \"col3\":1000}}" ,
        "{\"col\":{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":102,\"col3-2\":1002}}}" ,
        "{\"col\":{\"col1\":3, \"col2\":13, \"col3\":1003}}" } );
    String[] col2OriginalNames = new String[]{"col","col2"};
    String[] col3_1OriginalNames = new String[]{"col","col3","col3-1"};
    String[] col3_2OriginalNames = new String[]{"col","col3","col3-2"};
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "f_col2" , col2OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3-1" , col3_1OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3-2" , col3_2OriginalNames ) );

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 3 );
    assertNull( findColumnBinaryFromColumnName( "col" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3-1" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3-2" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenUnionSpreadTypeAndFilter() throws IOException {
    // schema: col spread<col1 int, col2 int , col3 union<spread<col3-1 int, col3-2 int>,int>>
    // |        col         |
    // | col1 | col2 | col3 |
    //               | col3-1 | col3-2 |
    // | 0    | 10   | 100 | 1000 |
    // | 1    | 11   |    1000    |
    // | 2    | 12   | 102 | 1002 |
    // | 3    | 13   |    1003    |
    // flattenSchema: f_col2 int , f_col3-1 int, f_col3-2 int
    // flteredFlattenSchema: f_col2 int, f_col3-2 int
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col\":{\"col1\":0, \"col2\":10, \"col3\":{\"col3-1\":100,\"col3-2\":1000}}}" ,
        "{\"col\":{\"col1\":1, \"col2\":11, \"col3\":1000}}" ,
        "{\"col\":{\"col1\":2, \"col2\":12, \"col3\":{\"col3-1\":102,\"col3-2\":1002}}}" ,
        "{\"col\":{\"col1\":3, \"col2\":13, \"col3\":1003}}" } );
    String[] col2OriginalNames = new String[]{"col","col2"};
    String[] col3_1OriginalNames = new String[]{"col","col3","col3-1"};
    String[] col3_2OriginalNames = new String[]{"col","col3","col3-2"};
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "f_col2" , col2OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3-1" , col3_1OriginalNames ) );
    flatten.add( new FlattenColumn( "f_col3-2" , col3_2OriginalNames ) );
    String[] newCol2OriginalName = flatten.getFlattenColumnName( "f_col2" );
    assertEquals( col2OriginalNames.length , newCol2OriginalName.length );
    for ( int i = 0; i < col2OriginalNames.length; i++ ) {
      assertEquals( col2OriginalNames[i] , newCol2OriginalName[i] );
    }
    String[] newCol3_2OriginalName = flatten.getFlattenColumnName( "f_col3-2" );
    assertEquals( col3_2OriginalNames.length , newCol3_2OriginalName.length );
    for ( int i = 0; i < col3_2OriginalNames.length; i++ ) {
      assertEquals( col3_2OriginalNames[i] , newCol3_2OriginalName[i] );
    }

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 2 );
    assertNull( findColumnBinaryFromColumnName( "col" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "f_col3-1" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col3-2" , flattenColumnBinaryList ) );
  }

  @Test
  public void T_flatten_equals_whenArraySpreadType() throws IOException {
    // schema: col spread<col1 int, col2 int , col3 array<spread<col3-1 int, col3-2 int>>>
    // |        col         |
    // | col1 | col2 | col3 |
    //               | col3-1 | col3-2 |
    // | 0    | 10   | 100 | 1000 |
    // | 1    | 11   | 101 | 1001 |
    // | 2    | 12   | 102 | 1002 |
    // | 3    | 13   | 103 | 1003 |
    // flattenSchema: f_col2 int , f_col3-1 int, f_col3-2 int
    // resultSchema: f_col2 int(f_col3-1, f_col3-2 is not found)
    List<ColumnBinary> binaryList = createReaderFromJsonString( new String[]{
        "{\"col\":{\"col1\":0, \"col2\":10, \"col3\":[{\"col3-1\":100,\"col3-2\":1000}]}}" ,
        "{\"col\":{\"col1\":1, \"col2\":11, \"col3\":[{\"col3-1\":101,\"col3-2\":1001}]}}" ,
        "{\"col\":{\"col1\":2, \"col2\":12, \"col3\":[{\"col3-1\":102,\"col3-2\":1002}]}}" ,
        "{\"col\":{\"col1\":3, \"col2\":13, \"col3\":[{\"col3-1\":103,\"col3-2\":1003}]}}" } );
    FlattenFunction flatten = new FlattenFunction();
    flatten.add( new FlattenColumn( "f_col2" , new String[]{"col","col2"} ) );
    flatten.add( new FlattenColumn( "f_col3-1" , new String[]{"col","col3","col3-1"} ) );
    flatten.add( new FlattenColumn( "f_col3-2" , new String[]{"col","col3","col3-2"} ) );

    List<ColumnBinary> flattenColumnBinaryList = flatten.flattenFromColumnBinary( binaryList );
    assertEquals( flattenColumnBinaryList.size() , 1 );
    assertNull( findColumnBinaryFromColumnName( "col" , flattenColumnBinaryList ) );
    assertNotNull( findColumnBinaryFromColumnName( "f_col2" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "f_col3-1" , flattenColumnBinaryList ) );
    assertNull( findColumnBinaryFromColumnName( "f_col3-2" , flattenColumnBinaryList ) );
  }

}
