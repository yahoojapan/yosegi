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

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;

import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
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
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import jp.co.yahoo.yosegi.message.objects.*;

public class TestMaxLengthBasedArrayColumnBinaryMaker{


  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    int loadCount =
        (columnBinary.loadIndex == null) ? columnBinary.rowCount : columnBinary.loadIndex.length;
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  @Test
  public void T_toBinary_equalsSetValue() throws IOException{
    IColumn column = new ArrayColumn( "array" );
    List<Object> value = new ArrayList<Object>();
    value.add( new StringObj( "a" ) );
    value.add( new StringObj( "b" ) );
    value.add( new StringObj( "c" ) );
    column.add( ColumnType.ARRAY , value , 0 );
    column.add( ColumnType.ARRAY , value , 1 );
    column.add( ColumnType.ARRAY , value , 2 );
    column.add( ColumnType.ARRAY , value , 3 );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = new MaxLengthBasedArrayColumnBinaryMaker();
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );

    assertEquals( columnBinary.columnName , "array" );
    assertEquals( columnBinary.rowCount , 4 );
    assertEquals( columnBinary.columnType , ColumnType.ARRAY );

    IColumn decodeColumn = toColumn(columnBinary);
    IColumn expandColumn = decodeColumn.getColumn(0);
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 1 );
    for( int i = 0 ; i < 3 * 4 ; i+=3 ){
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i).getRow() ) ).getString() , "a" );
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i+1).getRow() ) ).getString() , "b" );
      assertEquals( ( (PrimitiveObject)( expandColumn.get(i+2).getRow() ) ).getString() , "c" );
    }
    assertEquals( decodeColumn.getColumnKeys().size() , 0 );
    assertEquals( decodeColumn.getColumnSize() , 1 );
  }

}
