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

public class TestSpreadColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker" )
    );
  }

  private IColumn createSpreadColumnFromJsonString(
        final String targetClassName , final String[] jsonStrings ) throws IOException {
    JacksonMessageReader jsonReader = new JacksonMessageReader();
    SpreadColumn spreadColumn = new SpreadColumn( "test" );
    int addCount = 0;
    for ( String json : jsonStrings ) {
      spreadColumn.add( ColumnType.SPREAD , jsonReader.create( json ) , addCount );
      addCount++;
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , spreadColumn );

    return new YosegiLoaderFactory().create(
        columnBinary , addCount );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_load_childColumnEqualsJsonString( final String targetClassName ) throws IOException{
    IColumn column = createSpreadColumnFromJsonString( 
        targetClassName , 
        new String[]{
          "{\"col1\":\"a\", \"col2\":\"aa\", \"col3\":\"aaa\"}" ,
          "{\"col1\":\"b\", \"col2\":\"bb\", \"col3\":\"bbb\"}" ,
          "{\"col1\":\"c\", \"col2\":\"cc\", \"col3\":\"ccc\"}" ,
          "{\"col2\":\"dd\", \"col3\":\"ddd\"}" ,
          "{\"col3\":\"eee\"}" } );
    assertEquals( column.getColumnType() , ColumnType.SPREAD );
    assertEquals( column.size() , 5 );
    assertEquals( column.getColumnSize() , 3 );

    IColumn column1 = column.getColumn( "col1" );
    IColumn column2 = column.getColumn( "col2" );
    IColumn column3 = column.getColumn( "col3" );

    assertEquals( column1.getColumnType() , ColumnType.STRING );
    assertEquals( column2.getColumnType() , ColumnType.STRING );
    assertEquals( column3.getColumnType() , ColumnType.STRING );

    assertEquals( column1.size() , 5 );
    assertEquals( column2.size() , 5 );
    assertEquals( column3.size() , 5 );

    assertEquals( ( (PrimitiveObject)( column1.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column1.get(1).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column1.get(2).getRow() ) ).getString() , "c" );
    assertNull( ( (PrimitiveObject)( column1.get(3).getRow() ) ) );
    assertNull( ( (PrimitiveObject)( column1.get(4).getRow() ) ) );

    assertEquals( ( (PrimitiveObject)( column2.get(0).getRow() ) ).getString() , "aa" );
    assertEquals( ( (PrimitiveObject)( column2.get(1).getRow() ) ).getString() , "bb" );
    assertEquals( ( (PrimitiveObject)( column2.get(2).getRow() ) ).getString() , "cc" );
    assertEquals( ( (PrimitiveObject)( column2.get(3).getRow() ) ).getString() , "dd" );
    assertNull( ( (PrimitiveObject)( column2.get(4).getRow() ) ) );

    assertEquals( ( (PrimitiveObject)( column3.get(0).getRow() ) ).getString() , "aaa" );
    assertEquals( ( (PrimitiveObject)( column3.get(1).getRow() ) ).getString() , "bbb" );
    assertEquals( ( (PrimitiveObject)( column3.get(2).getRow() ) ).getString() , "ccc" );
    assertEquals( ( (PrimitiveObject)( column3.get(3).getRow() ) ).getString() , "ddd" );
    assertEquals( ( (PrimitiveObject)( column3.get(4).getRow() ) ).getString() , "eee" );
  }

}
