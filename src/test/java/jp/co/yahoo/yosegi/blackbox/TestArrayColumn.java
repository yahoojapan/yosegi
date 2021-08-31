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

public class TestArrayColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.MaxLengthBasedArrayColumnBinaryMaker" )
    );
  }

  private IColumn createArrayColumnFromJsonString(
        final String targetClassName , final String[] jsonStrings ) throws IOException {
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

    return new YosegiLoaderFactory().create(
        columnBinary , addCount );
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

}
