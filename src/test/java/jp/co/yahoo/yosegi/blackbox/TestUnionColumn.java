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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.inmemory.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestUnionColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.DumpUnionColumnBinaryMaker" )
    );
  }

  private IColumn createSpreadColumnFromJsonString(
        final String targetClassName , final int[] repetitions, final int loadSize ) throws IOException {
    IColumn firstColumn = new PrimitiveColumn( ColumnType.STRING , "UNION" );
    firstColumn.add( ColumnType.STRING , new StringObj( "a" ) , 0 );

    IColumn column = new UnionColumn( firstColumn );
    column.add( ColumnType.DOUBLE , new DoubleObj( 1.0 ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 2 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 1.0 ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "eee" ) , 4 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if (repetitions != null) {
      columnBinary.setRepetitions(repetitions, loadSize);
    }
    for ( ColumnBinary child : columnBinary.columnBinaryList ) {
      if (repetitions != null) {
        child.setRepetitions(repetitions, loadSize);
      }
    }
    try {
      return new YosegiLoaderFactory()
          .create(columnBinary, (loadSize > 0) ? loadSize : column.size());
    } catch ( Exception ex ) {
      ex.printStackTrace();
      throw ex;
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_load_childColumnEqualsJsonString( final String targetClassName ) throws IOException{
    IColumn column = createSpreadColumnFromJsonString( targetClassName , null, 0 );
    assertEquals( column.getColumnType() , ColumnType.UNION );
    assertEquals( column.size() , 5 );

    IColumn column1 = column.getColumn( ColumnType.STRING );
    IColumn column2 = column.getColumn( ColumnType.DOUBLE );

    assertEquals( column1.getColumnType() , ColumnType.STRING );
    assertEquals( column2.getColumnType() , ColumnType.DOUBLE );

    assertEquals( column1.size() , 5 );
    assertEquals( column2.size() , 5 );

    assertEquals( ( (PrimitiveObject)( column1.get(0).getRow() ) ).getString() , "a" );
    assertNull( ( (PrimitiveObject)( column1.get(1).getRow() ) ) );
    assertEquals( ( (PrimitiveObject)( column1.get(2).getRow() ) ).getString() , "c" );
    assertNull( ( (PrimitiveObject)( column1.get(3).getRow() ) ) );
    assertEquals( ( (PrimitiveObject)( column1.get(4).getRow() ) ).getString() , "eee" );

    assertNull( ( (PrimitiveObject)( column2.get(0).getRow() ) ) );
    assertEquals( ( (PrimitiveObject)( column2.get(1).getRow() ) ).getDouble() , 1.0 );
    assertNull( ( (PrimitiveObject)( column2.get(2).getRow() ) ) );
    assertEquals( ( (PrimitiveObject)( column2.get(3).getRow() ) ).getDouble() , 1.0 );
    assertNull( ( (PrimitiveObject)( column2.get(4).getRow() ) ) );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_load_childColumnEqualsJsonString_withRepetitions( final String targetClassName ) throws IOException{
    IColumn column = createSpreadColumnFromJsonString( targetClassName , new int[]{0,0,1,1,1,1,1}, 5 );
    assertEquals( column.getColumnType() , ColumnType.UNION );
    assertEquals( column.size() , 5 );

    IColumn column1 = column.getColumn( ColumnType.STRING );
    IColumn column2 = column.getColumn( ColumnType.DOUBLE );

    assertEquals( column1.getColumnType() , ColumnType.STRING );
    assertEquals( column2.getColumnType() , ColumnType.DOUBLE );

    assertEquals( column1.size() , 5 );
    assertEquals( column2.size() , 5 );

    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getDouble() , 1.0 );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "eee" );
    assertNull( ( (PrimitiveObject)( column.get(3).getRow() ) ) );
    assertNull( ( (PrimitiveObject)( column.get(4).getRow() ) ) );
  }

  @ParameterizedTest
  @MethodSource("data1")
  public void T_load_childColumnEqualsJsonString_withRepetitionsAndExpand(
      final String targetClassName) throws IOException {
    IColumn column =
        createSpreadColumnFromJsonString(targetClassName, new int[] {0, 0, 1, 2, 1, 1, 1}, 6);
    assertEquals(column.getColumnType(), ColumnType.UNION);
    assertEquals(column.size(), 6);

    IColumn column1 = column.getColumn(ColumnType.STRING);
    IColumn column2 = column.getColumn(ColumnType.DOUBLE);

    assertEquals(column1.getColumnType(), ColumnType.STRING);
    assertEquals(column2.getColumnType(), ColumnType.DOUBLE);

    assertEquals(column1.size(), 6);
    assertEquals(column2.size(), 6);

    assertEquals(((PrimitiveObject) (column.get(0).getRow())).getString(), "c");
    assertEquals(((PrimitiveObject) (column.get(1).getRow())).getDouble(), 1.0);
    assertEquals(((PrimitiveObject) (column.get(2).getRow())).getDouble(), 1.0);
    assertEquals(((PrimitiveObject) (column.get(3).getRow())).getString(), "eee");
    assertNull(((PrimitiveObject) (column.get(4).getRow())));
    assertNull(((PrimitiveObject) (column.get(5).getRow())));
  }
}
