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
import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import jp.co.yahoo.yosegi.config.Configuration;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;

public class TestBooleanPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.DumpBooleanColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBooleanColumnBinaryMaker" )
    );
  }

  public IColumn createNotNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "column" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 0 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 1 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 2 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 3 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 4 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 5 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 6 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 7 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 8 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 9 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return  FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createHasNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "column" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 0 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 4 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 8 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createLastCellColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BOOLEAN , "column" );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 10000 );

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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() , true );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean() , false );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean() , true );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean() , false );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean() , true );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getBoolean() , false );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getBoolean() , true );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getBoolean() , false );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getBoolean() , true );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getBoolean() , false );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getBoolean() , true );
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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean() , true );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean() , false );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getBoolean() , true );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getBoolean() , false );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_getPrimitiveObjectArray_1( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    IExpressionIndex indexList = new AllExpressionIndex( column.size() );
    PrimitiveObject[] objArray = column.getPrimitiveObjectArray( indexList , 0 , indexList.size() );

    assertEquals( objArray[0].getBoolean() , true );
    assertEquals( objArray[1].getBoolean() , false );
    assertEquals( objArray[2].getBoolean() , true );
    assertEquals( objArray[3].getBoolean() , false );
    assertEquals( objArray[4].getBoolean() , true );
    assertEquals( objArray[5].getBoolean() , false );
    assertEquals( objArray[6].getBoolean() , true );
    assertEquals( objArray[7].getBoolean() , false );
    assertEquals( objArray[8].getBoolean() , true );
    assertEquals( objArray[9].getBoolean() , false );
    assertEquals( objArray[10].getBoolean() , true );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_getPrimitiveObjectArray_2( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    List<Integer> list = new ArrayList<Integer>();
    list.add( Integer.valueOf( 0 ) );
    list.add( Integer.valueOf( 2 ) );
    list.add( Integer.valueOf( 4 ) );
    list.add( Integer.valueOf( 6 ) );
    list.add( Integer.valueOf( 8 ) );
    list.add( Integer.valueOf( 10 ) );
    IExpressionIndex indexList = new ListIndexExpressionIndex( list );
    PrimitiveObject[] objArray = column.getPrimitiveObjectArray( indexList , 0 , indexList.size() );

    assertEquals( objArray[0].getBoolean() , true );
    assertEquals( objArray[1].getBoolean() , true );
    assertEquals( objArray[2].getBoolean() , true );
    assertEquals( objArray[3].getBoolean() , true );
    assertEquals( objArray[4].getBoolean() , true );
    assertEquals( objArray[5].getBoolean() , true );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_getPrimitiveObjectArray_3( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    List<Integer> list = new ArrayList<Integer>();
    list.add( Integer.valueOf( 0 ) );
    list.add( Integer.valueOf( 2 ) );
    list.add( Integer.valueOf( 4 ) );
    list.add( Integer.valueOf( 6 ) );
    list.add( Integer.valueOf( 8 ) );
    list.add( Integer.valueOf( 10 ) );
    list.add( Integer.valueOf( 12 ) );
    IExpressionIndex indexList = new ListIndexExpressionIndex( list );
    PrimitiveObject[] objArray = column.getPrimitiveObjectArray( indexList , 0 , indexList.size() );

    assertEquals( objArray[0].getBoolean() , true );
    assertEquals( objArray[1].getBoolean() , true );
    assertEquals( objArray[2].getBoolean() , true );
    assertEquals( objArray[3].getBoolean() , true );
    assertEquals( objArray[4].getBoolean() , true );
    assertEquals( objArray[5].getBoolean() , true );
    assertNull( objArray[6] );
  }

}
