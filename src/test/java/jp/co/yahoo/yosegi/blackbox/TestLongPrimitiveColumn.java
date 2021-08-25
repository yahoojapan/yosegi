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

public class TestLongPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ) ,
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" )
    );
  }

  public IColumn createTestColumn( final String targetClassName , final long[] longArray ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    for ( int i = 0 ; i < longArray.length ; i++ ) {
      column.add( ColumnType.LONG , new LongObj( longArray[i] ) , i );
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNotNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( Long.MAX_VALUE ) , 0 );
    column.add( ColumnType.LONG , new LongObj( Long.MIN_VALUE ) , 1 );
    column.add( ColumnType.LONG , new LongObj( (long)-2000000000L ) , 2 );
    column.add( ColumnType.LONG , new LongObj( (long)-3000000000L ) , 3 );
    column.add( ColumnType.LONG , new LongObj( (long)-4000000000L ) , 4 );
    column.add( ColumnType.LONG , new LongObj( (long)-5000000000L ) , 5 );
    column.add( ColumnType.LONG , new LongObj( (long)-6000000000L ) , 6 );
    column.add( ColumnType.LONG , new LongObj( (long)7000000000L ) , 7 );
    column.add( ColumnType.LONG , new LongObj( (long)8000000000L ) , 8 );
    column.add( ColumnType.LONG , new LongObj( (long)9000000000L ) , 9 );
    column.add( ColumnType.LONG , new LongObj( (long)0 ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return  FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createHasNullColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( (long)0 ) , 0 );
    column.add( ColumnType.LONG , new LongObj( (long)4 ) , 4 );
    column.add( ColumnType.LONG , new LongObj( (long)8 ) , 8 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return FindColumnBinaryMaker.get( columnBinary.makerClassName ).toColumn( columnBinary );
  }

  public IColumn createLastCellColumn( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( Long.MAX_VALUE ) , 10000 );

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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() , Long.MAX_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getLong() , Long.MIN_VALUE );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getLong() , (long)-2000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getLong() , (long)-3000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getLong() , (long)-4000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getLong() , (long)-5000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getLong() , (long)-6000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getLong() , (long)7000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getLong() , (long)8000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getLong() , (long)9000000000L );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getLong() , (long)0 );
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
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getLong() , (long)0 );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getLong() , (long)4 );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getLong() , (long)8 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10000 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10000).getRow() ) ).getLong() , Long.MAX_VALUE );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withIntBit0( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt1( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      0L,
      0L,
      1L,
      1L,
      0L,
      0L,
      1L,
      1L,
      0L,
      0L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt2( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      0L,
      0L,
      1L,
      1L,
      2L,
      2L,
      3L,
      3L,
      0L,
      0L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt4( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      0L,
      0L,
      8L,
      8L,
      15L,
      15L,
      1L,
      2L,
      3L,
      4L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt8( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      (long)Byte.MAX_VALUE,
      (long)Byte.MIN_VALUE,
      0L,
      0L,
      64L,
      -64L,
      32L,
      -32L,
      16L,
      -16L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt16( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      (long)Short.MAX_VALUE,
      (long)Short.MIN_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MIN_VALUE,
      1L,
      1L,
      -1L,
      -1L,
      2L,
      2L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt24( final String targetClassName ) throws IOException{
    long max = 0xFFFFFFL;
    long[] longArray = new long[]{
      (long)Short.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      max,
      max,
      1L,
      1L,
      2L,
      2L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt32( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      (long)Integer.MAX_VALUE,
      (long)Integer.MIN_VALUE,
      (long)Short.MAX_VALUE,
      (long)Short.MIN_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MIN_VALUE,
      1L,
      1L,
      2L,
      2L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt40( final String targetClassName ) throws IOException{
    long max40 = 0xFFFFFFFFFFL;
    long[] longArray = new long[]{
      (long)Integer.MAX_VALUE,
      (long)Integer.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      0L,
      0L,
      max40,
      max40,
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt48( final String targetClassName ) throws IOException{
    long max48 = 0xFFFFFFFFFFL;
    long[] longArray = new long[]{
      (long)Integer.MAX_VALUE,
      (long)Integer.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      max48,
      max48,
      0L,
      0L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt56( final String targetClassName ) throws IOException{
    long max56 = 0xFFFFFFFFFFFFL;
    long[] longArray = new long[]{
      (long)Integer.MAX_VALUE,
      (long)Integer.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Short.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MAX_VALUE,
      max56,
      max56,
      0L,
      0L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_encodeAndDecode_equalsSetValue_withInt64( final String targetClassName ) throws IOException{
    long[] longArray = new long[]{
      (long)Long.MAX_VALUE,
      (long)Long.MIN_VALUE,
      (long)Integer.MAX_VALUE,
      (long)Integer.MIN_VALUE,
      (long)Short.MAX_VALUE,
      (long)Short.MIN_VALUE,
      (long)Byte.MAX_VALUE,
      (long)Byte.MIN_VALUE,
      0L,
      0L
    };
    IColumn column = createTestColumn( targetClassName , longArray );
    assertEquals( column.size() , 10 );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getLong() , longArray[i] );
    }
  }

}
