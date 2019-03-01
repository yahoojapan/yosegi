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

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestDoublePrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpDoubleColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDoubleColumnBinaryMaker" )
    );
  }

  public IColumn createNotNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( Double.MAX_VALUE ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( Double.MIN_VALUE ) , 1 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -200.0d ) , 2 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -300.1d ) , 3 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -400.2d ) , 4 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -500.3d ) , 5 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -600.4d ) , 6 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 700.5d ) , 7 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 800.6d ) , 8 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 900.7d ) , 9 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 0.0d ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return  FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createHasNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)0 ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)4 ) , 4 );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)8 ) , 8 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createLastCellColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( Double.MAX_VALUE ) , 10000 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
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

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_null_1( final String targetClassName ) throws IOException{
    IColumn column = createNullColumn( targetClassName );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
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

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getDouble() , Double.MAX_VALUE );
  }

}
