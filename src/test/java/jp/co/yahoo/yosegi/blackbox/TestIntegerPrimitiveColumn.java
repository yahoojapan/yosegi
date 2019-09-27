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

public class TestIntegerPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" )
    );
  }

  public IColumn createTestColumn( final String targetClassName , final int[] intArray ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    for ( int i = 0 ; i < intArray.length ; i++ ) {
      column.add( ColumnType.INTEGER , new IntegerObj( intArray[i] ) , i );
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNotNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( Integer.MAX_VALUE ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( Integer.MIN_VALUE ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)-20000 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)-30000 ) , 3 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)-40000 ) , 4 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)-50000 ) , 5 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)-60000 ) , 6 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)70000 ) , 7 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)80000 ) , 8 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)90000 ) , 9 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)0 ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return  FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createHasNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)0 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)4 ) , 4 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)8 ) , 8 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createLastCellColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( Integer.MAX_VALUE ) , 10000 );

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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() , Integer.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getInt() , Integer.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getInt() , (int)-20000 );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getInt() , (int)-30000 );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getInt() , (int)-40000 );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getInt() , (int)-50000 );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getInt() , (int)-60000 );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getInt() , (int)70000 );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getInt() , (int)80000 );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getInt() , (int)90000 );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getInt() , (int)0 );
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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getInt() , (int)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getInt() , (int)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getInt() , (int)8 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getInt() , Integer.MAX_VALUE );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withIntBit0( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt1( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      0,
      0,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      0
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt2( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      0,
      0,
      1,
      1,
      2,
      2,
      3,
      3,
      0,
      0
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt4( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      0,
      0,
      8,
      8,
      15,
      15,
      1,
      2,
      3,
      4
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt8( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      (int)Byte.MAX_VALUE,
      (int)Byte.MIN_VALUE,
      0,
      0,
      64,
      -64,
      32,
      -32,
      16,
      -16
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt16( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      (int)Short.MAX_VALUE,
      (int)Short.MIN_VALUE,
      (int)Byte.MAX_VALUE,
      (int)Byte.MIN_VALUE,
      1,
      1,
      -1,
      -1,
      2,
      2
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt24( final String targetClassName ) throws IOException{
    int max = 0xFFFFFF;
    int[] intArray = new int[]{
      (int)Short.MAX_VALUE,
      (int)Short.MAX_VALUE,
      (int)Byte.MAX_VALUE,
      (int)Byte.MAX_VALUE,
      max,
      max,
      1,
      1,
      2,
      2
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getInt() , intArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt32( final String targetClassName ) throws IOException{
    int[] intArray = new int[]{
      (int)Integer.MAX_VALUE,
      (int)Integer.MIN_VALUE,
      (int)Short.MAX_VALUE,
      (int)Short.MIN_VALUE,
      (int)Byte.MAX_VALUE,
      (int)Byte.MIN_VALUE,
      1,
      1,
      2,
      2
    };
    IColumn column = createTestColumn( targetClassName , intArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , intArray[i] );
    }
  }

}
