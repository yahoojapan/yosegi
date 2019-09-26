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

import java.util.Set;
import java.util.HashSet;

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

public class TestStringCellIndex{

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( createByteTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ),
      arguments( createByteTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ),
      arguments( createByteTestData( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ),
      arguments( createByteTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ) ),
      arguments( createByteTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" ) ),

      arguments( createShortTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ),
      arguments( createShortTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ),
      arguments( createShortTestData( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ),
      arguments( createShortTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ) ),
      arguments( createShortTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" ) ),

      arguments( createIntTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ),
      arguments( createIntTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ),
      arguments( createIntTestData( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ),
      arguments( createIntTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ) ),
      arguments( createIntTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" ) ),

      arguments( createLongTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ),
      arguments( createLongTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ),
      arguments( createLongTestData( "jp.co.yahoo.yosegi.binary.maker.RleLongColumnBinaryMaker" ) ),
      arguments( createLongTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayLongColumnBinaryMaker" ) ),
      arguments( createLongTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpLongColumnBinaryMaker" ) ),

      arguments( createFloatTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpFloatColumnBinaryMaker" ) ),
      arguments( createFloatTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeFloatColumnBinaryMaker" ) ),

      arguments( createDoubleTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpDoubleColumnBinaryMaker" ) ),
      arguments( createDoubleTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDoubleColumnBinaryMaker" ) ),
      arguments( createDoubleTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDoubleColumnBinaryMaker" ) ),

      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeStringColumnBinaryMaker" ) ),

      arguments( createBytesTestData( "jp.co.yahoo.yosegi.binary.maker.DumpBytesColumnBinaryMaker" ) )
    );
  }

  public static IColumn createByteTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( (byte)0 ) , 0 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)1 ) , 1 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)2 ) , 2 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)3 ) , 3 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)4 ) , 4 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)5 ) , 5 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)6 ) , 6 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)7 ) , 7 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)8 ) , 8 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)9 ) , 9 );

    column.add( ColumnType.BYTE , new ByteObj( (byte)20 ) , 20 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)21 ) , 21 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)22 ) , 22 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)23 ) , 23 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)24 ) , 24 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)25 ) , 25 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)26 ) , 26 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)27 ) , 27 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)28 ) , 28 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createShortTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column.add( ColumnType.SHORT , new ShortObj( (short)0 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)1 ) , 1 );
    column.add( ColumnType.SHORT , new ShortObj( (short)2 ) , 2 );
    column.add( ColumnType.SHORT , new ShortObj( (short)3 ) , 3 );
    column.add( ColumnType.SHORT , new ShortObj( (short)4 ) , 4 );
    column.add( ColumnType.SHORT , new ShortObj( (short)5 ) , 5 );
    column.add( ColumnType.SHORT , new ShortObj( (short)6 ) , 6 );
    column.add( ColumnType.SHORT , new ShortObj( (short)7 ) , 7 );
    column.add( ColumnType.SHORT , new ShortObj( (short)8 ) , 8 );
    column.add( ColumnType.SHORT , new ShortObj( (short)9 ) , 9 );

    column.add( ColumnType.SHORT , new ShortObj( (short)20 ) , 20 );
    column.add( ColumnType.SHORT , new ShortObj( (short)21 ) , 21 );
    column.add( ColumnType.SHORT , new ShortObj( (short)22 ) , 22 );
    column.add( ColumnType.SHORT , new ShortObj( (short)23 ) , 23 );
    column.add( ColumnType.SHORT , new ShortObj( (short)24 ) , 24 );
    column.add( ColumnType.SHORT , new ShortObj( (short)25 ) , 25 );
    column.add( ColumnType.SHORT , new ShortObj( (short)26 ) , 26 );
    column.add( ColumnType.SHORT , new ShortObj( (short)27 ) , 27 );
    column.add( ColumnType.SHORT , new ShortObj( (short)28 ) , 28 );
    column.add( ColumnType.SHORT , new ShortObj( (short)29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createIntTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( 0 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( 1 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( 2 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( 3 ) , 3 );
    column.add( ColumnType.INTEGER , new IntegerObj( 4 ) , 4 );
    column.add( ColumnType.INTEGER , new IntegerObj( 5 ) , 5 );
    column.add( ColumnType.INTEGER , new IntegerObj( 6 ) , 6 );
    column.add( ColumnType.INTEGER , new IntegerObj( 7 ) , 7 );
    column.add( ColumnType.INTEGER , new IntegerObj( 8 ) , 8 );
    column.add( ColumnType.INTEGER , new IntegerObj( 9 ) , 9 );

    column.add( ColumnType.INTEGER , new IntegerObj( 20 ) , 20 );
    column.add( ColumnType.INTEGER , new IntegerObj( 21 ) , 21 );
    column.add( ColumnType.INTEGER , new IntegerObj( 22 ) , 22 );
    column.add( ColumnType.INTEGER , new IntegerObj( 23 ) , 23 );
    column.add( ColumnType.INTEGER , new IntegerObj( 24 ) , 24 );
    column.add( ColumnType.INTEGER , new IntegerObj( 25 ) , 25 );
    column.add( ColumnType.INTEGER , new IntegerObj( 26 ) , 26 );
    column.add( ColumnType.INTEGER , new IntegerObj( 27 ) , 27 );
    column.add( ColumnType.INTEGER , new IntegerObj( 28 ) , 28 );
    column.add( ColumnType.INTEGER , new IntegerObj( 29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createLongTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( 0 ) , 0 );
    column.add( ColumnType.LONG , new LongObj( 1 ) , 1 );
    column.add( ColumnType.LONG , new LongObj( 2 ) , 2 );
    column.add( ColumnType.LONG , new LongObj( 3 ) , 3 );
    column.add( ColumnType.LONG , new LongObj( 4 ) , 4 );
    column.add( ColumnType.LONG , new LongObj( 5 ) , 5 );
    column.add( ColumnType.LONG , new LongObj( 6 ) , 6 );
    column.add( ColumnType.LONG , new LongObj( 7 ) , 7 );
    column.add( ColumnType.LONG , new LongObj( 8 ) , 8 );
    column.add( ColumnType.LONG , new LongObj( 9 ) , 9 );

    column.add( ColumnType.LONG , new LongObj( 20 ) , 20 );
    column.add( ColumnType.LONG , new LongObj( 21 ) , 21 );
    column.add( ColumnType.LONG , new LongObj( 22 ) , 22 );
    column.add( ColumnType.LONG , new LongObj( 23 ) , 23 );
    column.add( ColumnType.LONG , new LongObj( 24 ) , 24 );
    column.add( ColumnType.LONG , new LongObj( 25 ) , 25 );
    column.add( ColumnType.LONG , new LongObj( 26 ) , 26 );
    column.add( ColumnType.LONG , new LongObj( 27 ) , 27 );
    column.add( ColumnType.LONG , new LongObj( 28 ) , 28 );
    column.add( ColumnType.LONG , new LongObj( 29 ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createFloatTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column.add( ColumnType.FLOAT , new FloatObj( 0.0f ) , 0 );
    column.add( ColumnType.FLOAT , new FloatObj( 1.0f ) , 1 );
    column.add( ColumnType.FLOAT , new FloatObj( 2.0f ) , 2 );
    column.add( ColumnType.FLOAT , new FloatObj( 3.0f ) , 3 );
    column.add( ColumnType.FLOAT , new FloatObj( 4.0f ) , 4 );
    column.add( ColumnType.FLOAT , new FloatObj( 5.0f ) , 5 );
    column.add( ColumnType.FLOAT , new FloatObj( 6.0f ) , 6 );
    column.add( ColumnType.FLOAT , new FloatObj( 7.0f ) , 7 );
    column.add( ColumnType.FLOAT , new FloatObj( 8.0f ) , 8 );
    column.add( ColumnType.FLOAT , new FloatObj( 9.0f ) , 9 );

    column.add( ColumnType.FLOAT , new FloatObj( 20.0f ) , 20 );
    column.add( ColumnType.FLOAT , new FloatObj( 21.0f ) , 21 );
    column.add( ColumnType.FLOAT , new FloatObj( 22.0f ) , 22 );
    column.add( ColumnType.FLOAT , new FloatObj( 23.0f ) , 23 );
    column.add( ColumnType.FLOAT , new FloatObj( 24.0f ) , 24 );
    column.add( ColumnType.FLOAT , new FloatObj( 25.0f ) , 25 );
    column.add( ColumnType.FLOAT , new FloatObj( 26.0f ) , 26 );
    column.add( ColumnType.FLOAT , new FloatObj( 27.0f ) , 27 );
    column.add( ColumnType.FLOAT , new FloatObj( 28.0f ) , 28 );
    column.add( ColumnType.FLOAT , new FloatObj( 29.0f ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createDoubleTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( 0.0d ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 1.0d ) , 1 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 2.0d ) , 2 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 3.0d ) , 3 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 4.0d ) , 4 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 5.0d ) , 5 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 6.0d ) , 6 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 7.0d ) , 7 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 8.0d ) , 8 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 9.0d ) , 9 );

    column.add( ColumnType.DOUBLE , new DoubleObj( 20.0d ) , 20 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 21.0d ) , 21 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 22.0d ) , 22 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 23.0d ) , 23 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 24.0d ) , 24 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 25.0d ) , 25 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 26.0d ) , 26 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 27.0d ) , 27 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 28.0d ) , 28 );
    column.add( ColumnType.DOUBLE , new DoubleObj( 29.0d ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createStringTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "0" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "1" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "2" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "3" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "4" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "5" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "6" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "7" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "8" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "9" ) , 9 );

    column.add( ColumnType.STRING , new StringObj( "20" ) , 20 );
    column.add( ColumnType.STRING , new StringObj( "21" ) , 21 );
    column.add( ColumnType.STRING , new StringObj( "22" ) , 22 );
    column.add( ColumnType.STRING , new StringObj( "23" ) , 23 );
    column.add( ColumnType.STRING , new StringObj( "24" ) , 24 );
    column.add( ColumnType.STRING , new StringObj( "25" ) , 25 );
    column.add( ColumnType.STRING , new StringObj( "26" ) , 26 );
    column.add( ColumnType.STRING , new StringObj( "27" ) , 27 );
    column.add( ColumnType.STRING , new StringObj( "28" ) , 28 );
    column.add( ColumnType.STRING , new StringObj( "29" ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static IColumn createBytesTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "column" );
    column.add( ColumnType.BYTES , new BytesObj( "0".getBytes() ) , 0 );
    column.add( ColumnType.BYTES , new BytesObj( "1".getBytes() ) , 1 );
    column.add( ColumnType.BYTES , new BytesObj( "2".getBytes() ) , 2 );
    column.add( ColumnType.BYTES , new BytesObj( "3".getBytes() ) , 3 );
    column.add( ColumnType.BYTES , new BytesObj( "4".getBytes() ) , 4 );
    column.add( ColumnType.BYTES , new BytesObj( "5".getBytes() ) , 5 );
    column.add( ColumnType.BYTES , new BytesObj( "6".getBytes() ) , 6 );
    column.add( ColumnType.BYTES , new BytesObj( "7".getBytes() ) , 7 );
    column.add( ColumnType.BYTES , new BytesObj( "8".getBytes() ) , 8 );
    column.add( ColumnType.BYTES , new BytesObj( "9".getBytes() ) , 9 );

    column.add( ColumnType.BYTES , new BytesObj( "20".getBytes() ) , 20 );
    column.add( ColumnType.BYTES , new BytesObj( "21".getBytes() ) , 21 );
    column.add( ColumnType.BYTES , new BytesObj( "22".getBytes() ) , 22 );
    column.add( ColumnType.BYTES , new BytesObj( "23".getBytes() ) , 23 );
    column.add( ColumnType.BYTES , new BytesObj( "24".getBytes() ) , 24 );
    column.add( ColumnType.BYTES , new BytesObj( "25".getBytes() ) , 25 );
    column.add( ColumnType.BYTES , new BytesObj( "26".getBytes() ) , 26 );
    column.add( ColumnType.BYTES , new BytesObj( "27".getBytes() ) , 27 );
    column.add( ColumnType.BYTES , new BytesObj( "28".getBytes() ) , 28 );
    column.add( ColumnType.BYTES , new BytesObj( "29".getBytes() ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public static void dumpFilterResult( final boolean[] result ){
    System.out.println( "-----------------------" );
    System.out.println( "String cell index test result." );
    System.out.println( "-----------------------" );
    for( int i = 0 ; i < result.length ; i++ ){
      System.out.println( String.format( "index:%d = %s" , i , Boolean.toString( result[i] ) ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_perfectMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 23 };
    IFilter filter = new PerfectMatchStringFilter( "23" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_partialMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 2 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new PartialMatchStringFilter( "2" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_partialMatch_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 3 , 23 };
    IFilter filter = new PartialMatchStringFilter( "3" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_partialMatch_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 9 , 29 };
    IFilter filter = new PartialMatchStringFilter( "9" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_partialMatch_4( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 29 };
    IFilter filter = new PartialMatchStringFilter( "29" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_forwardMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 };
    IFilter filter = new ForwardMatchStringFilter( "0" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_forwardMatch_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 2 , 20 };
    IFilter filter = new ForwardMatchStringFilter( "2" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_backwardMatch_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 20 };
    IFilter filter = new BackwardMatchStringFilter( "0" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_backwardMatch_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 20 };
    IFilter filter = new BackwardMatchStringFilter( "20" );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 };
    IFilter filter = new RangeStringCompareFilter( "0" , true , "1" , true );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 1 };
    IFilter filter = new RangeStringCompareFilter( "0" , false , "1" , true );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_3( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 };
    IFilter filter = new RangeStringCompareFilter( "0" , true , "1" , false );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_4( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 1 };
    IFilter filter = new RangeStringCompareFilter( "0" , false , "2" , false );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_5( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new RangeStringCompareFilter( "0" , true , "1" , true , true );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_6( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new RangeStringCompareFilter( "0" , false , "1" , true , true );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_7( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 ,16 , 17 , 18 , 19 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new RangeStringCompareFilter( "0" , true , "1" , false , true );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_8( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15 , 16 , 17 , 18 , 19 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new RangeStringCompareFilter( "0" , false , "1" , false , true );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_dictionaryString_1( final IColumn column ) throws IOException{
    Set<String> dic = new HashSet<String>();
    dic.add( "9" );
    dic.add( "20" );
    int[] mustReadIndex = { 9 , 20 };
    IFilter filter = new StringDictionaryFilter( dic );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_dictionaryString_2( final IColumn column ) throws IOException{
    Set<String> dic = new HashSet<String>();
    dic.add( "0" );
    dic.add( "1" );
    dic.add( "2" );
    dic.add( "3" );
    dic.add( "4" );
    dic.add( "5" );
    dic.add( "6" );
    dic.add( "7" );
    dic.add( "8" );
    dic.add( "9" );
    dic.add( "20" );
    dic.add( "21" );
    dic.add( "22" );
    dic.add( "23" );
    dic.add( "24" );
    dic.add( "25" );
    dic.add( "26" );
    dic.add( "27" );
    dic.add( "28" );
    dic.add( "29" );
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new StringDictionaryFilter( dic );
    boolean[] filterResult = new boolean[30];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    //dumpFilterResult( filterResult );
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

}
