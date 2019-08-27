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

import java.util.*;

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

public class TestNullCellIndex{

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
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker" ) ),

      arguments( createBytesTestData( "jp.co.yahoo.yosegi.binary.maker.DumpBytesColumnBinaryMaker" ) ),

      arguments( createArrayTestData( "jp.co.yahoo.yosegi.binary.maker.DumpArrayColumnBinaryMaker" ) ),

      arguments( createArrayTestData( "jp.co.yahoo.yosegi.binary.maker.MaxLengthBasedArrayColumnBinaryMaker" ) ),

      arguments( createSpreadTestData( "jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker" ) ),

      arguments( createNullTestData( "jp.co.yahoo.yosegi.binary.maker.UnsupportedColumnBinaryMaker" ) )
    );
  }

  private static IColumn createByteTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-10 ) , 0 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-11 ) , 1 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-12 ) , 2 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-13 ) , 3 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-14 ) , 4 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-15 ) , 5 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-16 ) , 6 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-17 ) , 7 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-18 ) , 8 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-19 ) , 9 );

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

  private static IColumn createShortTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column.add( ColumnType.SHORT , new ShortObj( (short)-10 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-11 ) , 1 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-12 ) , 2 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-13 ) , 3 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-14 ) , 4 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-15 ) , 5 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-16 ) , 6 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-17 ) , 7 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-18 ) , 8 );
    column.add( ColumnType.SHORT , new ShortObj( (short)-19 ) , 9 );

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

  private static IColumn createIntTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( -10 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( -11 ) , 1 );
    column.add( ColumnType.INTEGER , new IntegerObj( -12 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( -13 ) , 3 );
    column.add( ColumnType.INTEGER , new IntegerObj( -14 ) , 4 );
    column.add( ColumnType.INTEGER , new IntegerObj( -15 ) , 5 );
    column.add( ColumnType.INTEGER , new IntegerObj( -16 ) , 6 );
    column.add( ColumnType.INTEGER , new IntegerObj( -17 ) , 7 );
    column.add( ColumnType.INTEGER , new IntegerObj( -18 ) , 8 );
    column.add( ColumnType.INTEGER , new IntegerObj( -19 ) , 9 );

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

  private static IColumn createLongTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( -10 ) , 0 );
    column.add( ColumnType.LONG , new LongObj( -11 ) , 1 );
    column.add( ColumnType.LONG , new LongObj( -12 ) , 2 );
    column.add( ColumnType.LONG , new LongObj( -13 ) , 3 );
    column.add( ColumnType.LONG , new LongObj( -14 ) , 4 );
    column.add( ColumnType.LONG , new LongObj( -15 ) , 5 );
    column.add( ColumnType.LONG , new LongObj( -16 ) , 6 );
    column.add( ColumnType.LONG , new LongObj( -17 ) , 7 );
    column.add( ColumnType.LONG , new LongObj( -18 ) , 8 );
    column.add( ColumnType.LONG , new LongObj( -19 ) , 9 );

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

  private static IColumn createFloatTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column.add( ColumnType.FLOAT , new FloatObj( -10.0f ) , 0 );
    column.add( ColumnType.FLOAT , new FloatObj( -11.0f ) , 1 );
    column.add( ColumnType.FLOAT , new FloatObj( -12.0f ) , 2 );
    column.add( ColumnType.FLOAT , new FloatObj( -13.0f ) , 3 );
    column.add( ColumnType.FLOAT , new FloatObj( -14.0f ) , 4 );
    column.add( ColumnType.FLOAT , new FloatObj( -15.0f ) , 5 );
    column.add( ColumnType.FLOAT , new FloatObj( -16.0f ) , 6 );
    column.add( ColumnType.FLOAT , new FloatObj( -17.0f ) , 7 );
    column.add( ColumnType.FLOAT , new FloatObj( -18.0f ) , 8 );
    column.add( ColumnType.FLOAT , new FloatObj( -19.0f ) , 9 );

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

  private static IColumn createDoubleTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( -10.0d ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -11.0d ) , 1 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -12.0d ) , 2 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -13.0d ) , 3 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -14.0d ) , 4 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -15.0d ) , 5 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -16.0d ) , 6 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -17.0d ) , 7 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -18.0d ) , 8 );
    column.add( ColumnType.DOUBLE , new DoubleObj( -19.0d ) , 9 );

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

  private static IColumn createStringTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "-10" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "-11" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "-12" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "-13" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "-14" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "-15" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "-16" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "-17" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "-18" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "-19" ) , 9 );

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

  private static IColumn createBytesTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTES , "column" );
    column.add( ColumnType.BYTES , new BytesObj( "-10".getBytes() ) , 0 );
    column.add( ColumnType.BYTES , new BytesObj( "-11".getBytes() ) , 1 );
    column.add( ColumnType.BYTES , new BytesObj( "-12".getBytes() ) , 2 );
    column.add( ColumnType.BYTES , new BytesObj( "-13".getBytes() ) , 3 );
    column.add( ColumnType.BYTES , new BytesObj( "-14".getBytes() ) , 4 );
    column.add( ColumnType.BYTES , new BytesObj( "-15".getBytes() ) , 5 );
    column.add( ColumnType.BYTES , new BytesObj( "-16".getBytes() ) , 6 );
    column.add( ColumnType.BYTES , new BytesObj( "-17".getBytes() ) , 7 );
    column.add( ColumnType.BYTES , new BytesObj( "-18".getBytes() ) , 8 );
    column.add( ColumnType.BYTES , new BytesObj( "-19".getBytes() ) , 9 );

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

  private static IColumn createBooleanTestData( final String targetClassName ) throws IOException{
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

    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 20 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 21 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 22 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 23 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 24 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 25 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 26 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 27 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 28 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  private static IColumn createArrayTestData( final String targetClassName ) throws IOException{
    List<Object> data = new ArrayList<Object>();
    data.add( new BooleanObj( true ) );
    IColumn column = new ArrayColumn( "column" );
    column.add( ColumnType.ARRAY , data , 0 );
    column.add( ColumnType.ARRAY , data , 1 );
    column.add( ColumnType.ARRAY , data , 2 );
    column.add( ColumnType.ARRAY , data , 3 );
    column.add( ColumnType.ARRAY , data , 4 );
    column.add( ColumnType.ARRAY , data , 5 );
    column.add( ColumnType.ARRAY , data , 6 );
    column.add( ColumnType.ARRAY , data , 7 );
    column.add( ColumnType.ARRAY , data , 8 );
    column.add( ColumnType.ARRAY , data , 9 );

    column.add( ColumnType.ARRAY , data , 20 );
    column.add( ColumnType.ARRAY , data , 21 );
    column.add( ColumnType.ARRAY , data , 22 );
    column.add( ColumnType.ARRAY , data , 23 );
    column.add( ColumnType.ARRAY , data , 24 );
    column.add( ColumnType.ARRAY , data , 25 );
    column.add( ColumnType.ARRAY , data , 26 );
    column.add( ColumnType.ARRAY , data , 27 );
    column.add( ColumnType.ARRAY , data , 28 );
    column.add( ColumnType.ARRAY , data , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  private static IColumn createSpreadTestData( final String targetClassName ) throws IOException{
    Map<Object,Object> data = new HashMap<Object,Object>();
    data.put( "t" , new BooleanObj( true ) );
    IColumn column = new SpreadColumn( "column" );
    column.add( ColumnType.SPREAD , data , 0 );
    column.add( ColumnType.SPREAD , data , 1 );
    column.add( ColumnType.SPREAD , data , 2 );
    column.add( ColumnType.SPREAD , data , 3 );
    column.add( ColumnType.SPREAD , data , 4 );
    column.add( ColumnType.SPREAD , data , 5 );
    column.add( ColumnType.SPREAD , data , 6 );
    column.add( ColumnType.SPREAD , data , 7 );
    column.add( ColumnType.SPREAD , data , 8 );
    column.add( ColumnType.SPREAD , data , 9 );

    column.add( ColumnType.SPREAD , data , 20 );
    column.add( ColumnType.SPREAD , data , 21 );
    column.add( ColumnType.SPREAD , data , 22 );
    column.add( ColumnType.SPREAD , data , 23 );
    column.add( ColumnType.SPREAD , data , 24 );
    column.add( ColumnType.SPREAD , data , 25 );
    column.add( ColumnType.SPREAD , data , 26 );
    column.add( ColumnType.SPREAD , data , 27 );
    column.add( ColumnType.SPREAD , data , 28 );
    column.add( ColumnType.SPREAD , data , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  private static IColumn createNullTestData( final String targetClassName ) throws IOException{
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

    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 20 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 21 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 22 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 23 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 24 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 25 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 26 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 27 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( true ) , 28 );
    column.add( ColumnType.BOOLEAN , new BooleanObj( false ) , 29 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return maker.toColumn( columnBinary );
  }

  public void dumpFilterResult( final boolean[] result ){
    System.out.println( "-----------------------" );
    System.out.println( "Integer cell index test result." );
    System.out.println( "-----------------------" );
    for( int i = 0 ; i < result.length ; i++ ){
      System.out.println( String.format( "index:%d = %s" , i , Boolean.toString( result[i] ) ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_null_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 10 , 11 , 12 , 13 , 14 , 15 , 16 ,17 , 18 , 19 };
    IFilter filter = new NullFilter( column.getColumnType() );
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
  public void T_null_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 10 , 11 , 12 , 13 , 14 , 15 , 16 ,17 , 18 , 19 , 30 , 31 , 32 , 33 , 34 };
    IFilter filter = new NullFilter( column.getColumnType() );
    boolean[] filterResult = new boolean[35];
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
  public void T_notnull_1( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new NotNullFilter( column.getColumnType() );
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
  public void T_notnull_2( final IColumn column ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 };
    IFilter filter = new NotNullFilter( column.getColumnType() );
    boolean[] filterResult = new boolean[35];
    filterResult = column.filter( filter , filterResult );
    if( filterResult == null ){
      assertTrue( true );
      return;
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( filterResult[mustReadIndex[i]] );
    }
  }

}
