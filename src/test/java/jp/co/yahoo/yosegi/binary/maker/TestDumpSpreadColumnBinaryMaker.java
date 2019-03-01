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
package jp.co.yahoo.yosegi.binary.maker;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;

import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestDumpSpreadColumnBinaryMaker {

  @Test
  public void T_toBinary_1() throws IOException{
    IColumn column = new SpreadColumn( "SPREAD" );
    Map<String,Object> data = new HashMap<String,Object>();
    data.put( "a" , new StringObj( "a" ) );
    data.put( "b" , new StringObj( "b" ) );
    column.add( ColumnType.SPREAD , data , 0 );
    column.add( ColumnType.SPREAD , data , 1 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new DumpSpreadColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    assertEquals( columnBinary.columnName , "SPREAD" );
    assertEquals( columnBinary.rowCount , 2 );
    assertEquals( columnBinary.columnType , ColumnType.SPREAD );

    IColumn decodeColumn = maker.toColumn( columnBinary );
    assertEquals( decodeColumn.getColumnKeys().size() , 2 );
    assertEquals( decodeColumn.getColumnSize() , 2 );

    IColumn aColumn = decodeColumn.getColumn( "a" );
    IColumn bColumn = decodeColumn.getColumn( "b" );
    for( int i = 0 ; i < 2 ; i++ ){
      assertEquals( "a" , ( (PrimitiveObject)( aColumn.get(i).getRow() ) ).getString() );
      assertEquals( "b" , ( (PrimitiveObject)( bColumn.get(i).getRow() ) ).getString() );
    }

    assertEquals( decodeColumn.getColumnKeys().size() , 2 );
    assertEquals( decodeColumn.getColumnSize() , 2 );
  }
}
