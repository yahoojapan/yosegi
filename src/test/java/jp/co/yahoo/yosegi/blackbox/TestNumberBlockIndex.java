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
import jp.co.yahoo.yosegi.blockindex.*;

public class TestNumberBlockIndex{

  public static IBlockIndex[] createBlockIndex() throws IOException{
    return  new IBlockIndex[] {
      createByteTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ,
      createByteTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ,

      createShortTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ,
      createShortTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ,

      createIntegerTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ,
      createIntegerTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ,

      createLongTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpLongColumnBinaryMaker" ) ,
      createLongTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeLongColumnBinaryMaker" ) ,

      createFloatTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpFloatColumnBinaryMaker" ) ,
      createFloatTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeFloatColumnBinaryMaker" ) ,

      createDoubleTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeRangeDumpDoubleColumnBinaryMaker" ) ,
      createDoubleTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDoubleColumnBinaryMaker" ) ,
    };
  }

  public static Stream<Arguments> data1() throws IOException{
    IBlockIndex[] byteClassNames = createBlockIndex();

    Object[] filter = new Object[6];
    filter[0] = createByteFilter();
    filter[1] = createShortFilter();
    filter[2] = createIntegerFilter();
    filter[3] = createLongFilter();
    filter[4] = createFloatFilter();
    filter[5] = createDoubleFilter();

    Arguments[] result = new Arguments[byteClassNames.length * filter.length];
    for( int i = 0 ; i < byteClassNames.length ; i++ ){
      int index = i * filter.length;
      for( int ii = 0 ; ii < filter.length ; ii++ ){
        result[index+ii] = arguments( byteClassNames[i] , filter[ii] );
      }
    }
    return Stream.of( result );
  }

  public static IFilter[] createByteFilter() throws IOException{
    return new IFilter[]{
      new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)5 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)10 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new ByteObj( (byte)-10 ) ),
      new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)0 ) ),
      new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)-9 ) ),
      new NumberFilter( NumberFilterType.LT , new ByteObj( (byte)-5 ) ),
      new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)0 ) ),
      new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)-10 ) ),
      new NumberFilter( NumberFilterType.LE , new ByteObj( (byte)-5 ) ),
      new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)0 ) ),
      new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)9 ) ),
      new NumberFilter( NumberFilterType.GT , new ByteObj( (byte)5 ) ),
      new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)0 ) ),
      new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)10 ) ),
      new NumberFilter( NumberFilterType.GE , new ByteObj( (byte)5 ) ),
      new NumberRangeFilter( new ByteObj( (byte)5 ) , true , new ByteObj( (byte)10 ) , true ),
      new NumberRangeFilter( new ByteObj( (byte)5 ) , false , new ByteObj( (byte)10 ) , true ),
      new NumberRangeFilter( new ByteObj( (byte)5 ) , true , new ByteObj( (byte)10 ) , false ),
      new NumberRangeFilter( new ByteObj( (byte)5 ) , false , new ByteObj( (byte)10 ) , false ),
      new NumberRangeFilter( true , new ByteObj( (byte)5 ) , true , new ByteObj( (byte)10 ) , true ),
      new NumberRangeFilter( true , new ByteObj( (byte)5 ) , false , new ByteObj( (byte)10 ) , true ),
      new NumberRangeFilter( true , new ByteObj( (byte)5 ) , true , new ByteObj( (byte)10 ) , false ),
      new NumberRangeFilter( true , new ByteObj( (byte)5 ) , false , new ByteObj( (byte)10 ) , false ),
    };
  }

  public static IFilter[] createShortFilter() throws IOException{
    return new IFilter[]{
      new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)5 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)10 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)-10 ) ),
      new NumberFilter( NumberFilterType.LT , new ShortObj( (short)0 ) ),
      new NumberFilter( NumberFilterType.LT , new ShortObj( (short)-9 ) ),
      new NumberFilter( NumberFilterType.LT , new ShortObj( (short)-5 ) ),
      new NumberFilter( NumberFilterType.LE , new ShortObj( (short)0 ) ),
      new NumberFilter( NumberFilterType.LE , new ShortObj( (short)-10 ) ),
      new NumberFilter( NumberFilterType.LE , new ShortObj( (short)-5 ) ),
      new NumberFilter( NumberFilterType.GT , new ShortObj( (short)0 ) ),
      new NumberFilter( NumberFilterType.GT , new ShortObj( (short)9 ) ),
      new NumberFilter( NumberFilterType.GT , new ShortObj( (short)5 ) ),
      new NumberFilter( NumberFilterType.GE , new ShortObj( (short)0 ) ),
      new NumberFilter( NumberFilterType.GE , new ShortObj( (short)10 ) ),
      new NumberFilter( NumberFilterType.GE , new ShortObj( (short)5 ) ),
      new NumberRangeFilter( new ShortObj( (short)5 ) , true , new ShortObj( (short)10 ) , true ),
      new NumberRangeFilter( new ShortObj( (short)5 ) , false , new ShortObj( (short)10 ) , true ),
      new NumberRangeFilter( new ShortObj( (short)5 ) , true , new ShortObj( (short)10 ) , false ),
      new NumberRangeFilter( new ShortObj( (short)5 ) , false , new ShortObj( (short)10 ) , false ),
      new NumberRangeFilter( true , new ShortObj( (short)5 ) , true , new ShortObj( (short)10 ) , true ),
      new NumberRangeFilter( true , new ShortObj( (short)5 ) , false , new ShortObj( (short)10 ) , true ),
      new NumberRangeFilter( true , new ShortObj( (short)5 ) , true , new ShortObj( (short)10 ) , false ),
      new NumberRangeFilter( true , new ShortObj( (short)5 ) , false , new ShortObj( (short)10 ) , false ),
    };
  }

  public static IFilter[] createIntegerFilter() throws IOException{
    return new IFilter[]{
      new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( 5 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( 10 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new IntegerObj( -10 ) ),
      new NumberFilter( NumberFilterType.LT , new IntegerObj( 0 ) ),
      new NumberFilter( NumberFilterType.LT , new IntegerObj( -9 ) ),
      new NumberFilter( NumberFilterType.LT , new IntegerObj( -5 ) ),
      new NumberFilter( NumberFilterType.LE , new IntegerObj( 0 ) ),
      new NumberFilter( NumberFilterType.LE , new IntegerObj( -10 ) ),
      new NumberFilter( NumberFilterType.LE , new IntegerObj( -5 ) ),
      new NumberFilter( NumberFilterType.GT , new IntegerObj( 0 ) ),
      new NumberFilter( NumberFilterType.GT , new IntegerObj( 9 ) ),
      new NumberFilter( NumberFilterType.GT , new IntegerObj( 5 ) ),
      new NumberFilter( NumberFilterType.GE , new IntegerObj( 0 ) ),
      new NumberFilter( NumberFilterType.GE , new IntegerObj( 10 ) ),
      new NumberFilter( NumberFilterType.GE , new IntegerObj( 5 ) ),
      new NumberRangeFilter( new IntegerObj( 5 ) , true , new IntegerObj( 10 ) , true ),
      new NumberRangeFilter( new IntegerObj( 5 ) , false , new IntegerObj( 10 ) , true ),
      new NumberRangeFilter( new IntegerObj( 5 ) , true , new IntegerObj( 10 ) , false ),
      new NumberRangeFilter( new IntegerObj( 5 ) , false , new IntegerObj( 10 ) , false ),
      new NumberRangeFilter( true , new IntegerObj( 5 ) , true , new IntegerObj( 10 ) , true ),
      new NumberRangeFilter( true , new IntegerObj( 5 ) , false , new IntegerObj( 10 ) , true ),
      new NumberRangeFilter( true , new IntegerObj( 5 ) , true , new IntegerObj( 10 ) , false ),
      new NumberRangeFilter( true , new IntegerObj( 5 ) , false , new IntegerObj( 10 ) , false ),
    };
  }

  public static IFilter[] createLongFilter() throws IOException{
    return new IFilter[]{
      new NumberFilter( NumberFilterType.EQUAL , new LongObj( 5 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new LongObj( 10 ) ),
      new NumberFilter( NumberFilterType.EQUAL , new LongObj( -10 ) ),
      new NumberFilter( NumberFilterType.LT , new LongObj( 0 ) ),
      new NumberFilter( NumberFilterType.LT , new LongObj( -9 ) ),
      new NumberFilter( NumberFilterType.LT , new LongObj( -5 ) ),
      new NumberFilter( NumberFilterType.LE , new LongObj( 0 ) ),
      new NumberFilter( NumberFilterType.LE , new LongObj( -10 ) ),
      new NumberFilter( NumberFilterType.LE , new LongObj( -5 ) ),
      new NumberFilter( NumberFilterType.GT , new LongObj( 0 ) ),
      new NumberFilter( NumberFilterType.GT , new LongObj( 9 ) ),
      new NumberFilter( NumberFilterType.GT , new LongObj( 5 ) ),
      new NumberFilter( NumberFilterType.GE , new LongObj( 0 ) ),
      new NumberFilter( NumberFilterType.GE , new LongObj( 10 ) ),
      new NumberFilter( NumberFilterType.GE , new LongObj( 5 ) ),
      new NumberRangeFilter( new LongObj( 5 ) , true , new LongObj( 10 ) , true ),
      new NumberRangeFilter( new LongObj( 5 ) , false , new LongObj( 10 ) , true ),
      new NumberRangeFilter( new LongObj( 5 ) , true , new LongObj( 10 ) , false ),
      new NumberRangeFilter( new LongObj( 5 ) , false , new LongObj( 10 ) , false ),
      new NumberRangeFilter( true , new LongObj( 5 ) , true , new LongObj( 10 ) , true ),
      new NumberRangeFilter( true , new LongObj( 5 ) , false , new LongObj( 10 ) , true ),
      new NumberRangeFilter( true , new LongObj( 5 ) , true , new LongObj( 10 ) , false ),
      new NumberRangeFilter( true , new LongObj( 5 ) , false , new LongObj( 10 ) , false ),
    };
  }

  public static IFilter[] createFloatFilter() throws IOException{
    return new IFilter[]{
      new NumberFilter( NumberFilterType.EQUAL , new FloatObj( 5f ) ),
      new NumberFilter( NumberFilterType.EQUAL , new FloatObj( 10f ) ),
      new NumberFilter( NumberFilterType.EQUAL , new FloatObj( -10f ) ),
      new NumberFilter( NumberFilterType.LT , new FloatObj( 0f ) ),
      new NumberFilter( NumberFilterType.LT , new FloatObj( -9f ) ),
      new NumberFilter( NumberFilterType.LT , new FloatObj( -5f ) ),
      new NumberFilter( NumberFilterType.LE , new FloatObj( 0f ) ),
      new NumberFilter( NumberFilterType.LE , new FloatObj( -10f ) ),
      new NumberFilter( NumberFilterType.LE , new FloatObj( -5f ) ),
      new NumberFilter( NumberFilterType.GT , new FloatObj( 0f ) ),
      new NumberFilter( NumberFilterType.GT , new FloatObj( 9f ) ),
      new NumberFilter( NumberFilterType.GT , new FloatObj( 5f ) ),
      new NumberFilter( NumberFilterType.GE , new FloatObj( 0f ) ),
      new NumberFilter( NumberFilterType.GE , new FloatObj( 10f ) ),
      new NumberFilter( NumberFilterType.GE , new FloatObj( 5f ) ),
      new NumberRangeFilter( new FloatObj( 5f ) , true , new FloatObj( 10f ) , true ),
      new NumberRangeFilter( new FloatObj( 5f ) , false , new FloatObj( 10f ) , true ),
      new NumberRangeFilter( new FloatObj( 5f ) , true , new FloatObj( 10f ) , false ),
      new NumberRangeFilter( new FloatObj( 5f ) , false , new FloatObj( 10f ) , false ),
      new NumberRangeFilter( true , new FloatObj( 5f ) , true , new FloatObj( 10f ) , true ),
      new NumberRangeFilter( true , new FloatObj( 5f ) , false , new FloatObj( 10f ) , true ),
      new NumberRangeFilter( true , new FloatObj( 5f ) , true , new FloatObj( 10f ) , false ),
      new NumberRangeFilter( true , new FloatObj( 5f ) , false , new FloatObj( 10f ) , false ),
    };
  }

  public static IFilter[] createDoubleFilter() throws IOException{
    return new IFilter[]{
      new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( 5d ) ),
      new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( 10d ) ),
      new NumberFilter( NumberFilterType.EQUAL , new DoubleObj( -10d ) ),
      new NumberFilter( NumberFilterType.LT , new DoubleObj( 0d ) ),
      new NumberFilter( NumberFilterType.LT , new DoubleObj( -9d ) ),
      new NumberFilter( NumberFilterType.LT , new DoubleObj( -5d ) ),
      new NumberFilter( NumberFilterType.LE , new DoubleObj( 0d ) ),
      new NumberFilter( NumberFilterType.LE , new DoubleObj( -10d ) ),
      new NumberFilter( NumberFilterType.LE , new DoubleObj( -5d ) ),
      new NumberFilter( NumberFilterType.GT , new DoubleObj( 0d ) ),
      new NumberFilter( NumberFilterType.GT , new DoubleObj( 9d ) ),
      new NumberFilter( NumberFilterType.GT , new DoubleObj( 5d ) ),
      new NumberFilter( NumberFilterType.GE , new DoubleObj( 0d ) ),
      new NumberFilter( NumberFilterType.GE , new DoubleObj( 10d ) ),
      new NumberFilter( NumberFilterType.GE , new DoubleObj( 5d ) ),
      new NumberRangeFilter( new DoubleObj( 5d ) , true , new DoubleObj( 10d ) , true ),
      new NumberRangeFilter( new DoubleObj( 5d ) , false , new DoubleObj( 10d ) , true ),
      new NumberRangeFilter( new DoubleObj( 5d ) , true , new DoubleObj( 10d ) , false ),
      new NumberRangeFilter( new DoubleObj( 5d ) , false , new DoubleObj( 10d ) , false ),
      new NumberRangeFilter( true , new DoubleObj( 5d ) , true , new DoubleObj( 10d ) , true ),
      new NumberRangeFilter( true , new DoubleObj( 5d ) , false , new DoubleObj( 10d ) , true ),
      new NumberRangeFilter( true , new DoubleObj( 5d ) , true , new DoubleObj( 10d ) , false ),
      new NumberRangeFilter( true , new DoubleObj( 5d ) , false , new DoubleObj( 10d ) , false ),
    };
  }

  public static IBlockIndex createByteTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column.add( ColumnType.BYTE , new ByteObj( (byte)-10 ) , 0 );
    column.add( ColumnType.BYTE , new ByteObj( (byte)10 ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column2.add( ColumnType.BYTE , new ByteObj( (byte)-5 ) , 0 );
    column2.add( ColumnType.BYTE , new ByteObj( (byte)0 ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column3.add( ColumnType.BYTE , new ByteObj( (byte)0 ) , 0 );
    column3.add( ColumnType.BYTE , new ByteObj( (byte)5 ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.BYTE , "column" );
    column4.add( ColumnType.BYTE , new ByteObj( (byte)-3 ) , 0 );
    column4.add( ColumnType.BYTE , new ByteObj( (byte)3 ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , column4 );

    FindColumnBinaryMaker.get( c1.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c2.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  public static IBlockIndex createShortTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column.add( ColumnType.SHORT , new ShortObj( (short)-10 ) , 0 );
    column.add( ColumnType.SHORT , new ShortObj( (short)10 ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column2.add( ColumnType.SHORT , new ShortObj( (short)-5 ) , 0 );
    column2.add( ColumnType.SHORT , new ShortObj( (short)0 ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column3.add( ColumnType.SHORT , new ShortObj( (short)0 ) , 0 );
    column3.add( ColumnType.SHORT , new ShortObj( (short)5 ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.SHORT , "column" );
    column4.add( ColumnType.SHORT , new ShortObj( (short)-3 ) , 0 );
    column4.add( ColumnType.SHORT , new ShortObj( (short)3 ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , column4 );

    FindColumnBinaryMaker.get( c1.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c2.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  public static IBlockIndex createIntegerTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)-10 ) , 0 );
    column.add( ColumnType.INTEGER , new IntegerObj( (int)10 ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column2.add( ColumnType.INTEGER , new IntegerObj( (int)-5 ) , 0 );
    column2.add( ColumnType.INTEGER , new IntegerObj( (int)0 ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column3.add( ColumnType.INTEGER , new IntegerObj( (int)0 ) , 0 );
    column3.add( ColumnType.INTEGER , new IntegerObj( (int)5 ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.INTEGER , "column" );
    column4.add( ColumnType.INTEGER , new IntegerObj( (int)-3 ) , 0 );
    column4.add( ColumnType.INTEGER , new IntegerObj( (int)3 ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , column4 );

    FindColumnBinaryMaker.get( c1.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c2.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  public static IBlockIndex createLongTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.LONG , "column" );
    column.add( ColumnType.LONG , new LongObj( (long)-10 ) , 0 );
    column.add( ColumnType.LONG , new LongObj( (long)10 ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.LONG , "column" );
    column2.add( ColumnType.LONG , new LongObj( (long)-5 ) , 0 );
    column2.add( ColumnType.LONG , new LongObj( (long)0 ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.LONG , "column" );
    column3.add( ColumnType.LONG , new LongObj( (long)0 ) , 0 );
    column3.add( ColumnType.LONG , new LongObj( (long)5 ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.LONG , "column" );
    column4.add( ColumnType.LONG , new LongObj( (long)-3 ) , 0 );
    column4.add( ColumnType.LONG , new LongObj( (long)3 ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , column4 );

    FindColumnBinaryMaker.get( c1.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c2.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  public static IBlockIndex createFloatTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column.add( ColumnType.FLOAT , new FloatObj( (float)-10 ) , 0 );
    column.add( ColumnType.FLOAT , new FloatObj( (float)10 ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column2.add( ColumnType.FLOAT , new FloatObj( (float)-5 ) , 0 );
    column2.add( ColumnType.FLOAT , new FloatObj( (float)0 ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column3.add( ColumnType.FLOAT , new FloatObj( (float)0 ) , 0 );
    column3.add( ColumnType.FLOAT , new FloatObj( (float)5 ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.FLOAT , "column" );
    column4.add( ColumnType.FLOAT , new FloatObj( (float)-3 ) , 0 );
    column4.add( ColumnType.FLOAT , new FloatObj( (float)3 ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , column4 );

    FindColumnBinaryMaker.get( c1.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c2.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  public static IBlockIndex createDoubleTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)-10 ) , 0 );
    column.add( ColumnType.DOUBLE , new DoubleObj( (double)10 ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column2.add( ColumnType.DOUBLE , new DoubleObj( (double)-5 ) , 0 );
    column2.add( ColumnType.DOUBLE , new DoubleObj( (double)0 ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column3.add( ColumnType.DOUBLE , new DoubleObj( (double)0 ) , 0 );
    column3.add( ColumnType.DOUBLE , new DoubleObj( (double)5 ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.DOUBLE , "column" );
    column4.add( ColumnType.DOUBLE , new DoubleObj( (double)-3 ) , 0 );
    column4.add( ColumnType.DOUBLE , new DoubleObj( (double)3 ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , column4 );

    FindColumnBinaryMaker.get( c1.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c2.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_equal_1( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 2 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[0] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_equal_2( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[1] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_equal_3( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[2] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lt_1( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[3] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lt_2( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[4] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lt_3( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[5] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_le_1( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[6] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_le_2( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[7] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_le_3( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[8] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_gt_1( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[9] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_gt_2( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[10] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_gt_3( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[11] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_ge_1( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[12] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_ge_2( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[13] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_ge_3( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 2 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[14] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_1( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[15] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_2( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 2 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[16] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_3( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[17] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_4( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 2 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[18] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_5( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[19] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_6( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[20] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_7( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[21] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_range_8( final IBlockIndex index , final IFilter[] filter ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 };
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter[22] );
    if( resultIndexList == null ){
      assertTrue( true );
      return;
    }
    Set<Integer> dic = new HashSet<Integer>();
    for( Integer i : resultIndexList ){
      dic.add( i );
    }
    for( int i = 0 ; i < mustReadIndex.length ; i++ ){
      assertTrue( dic.contains( mustReadIndex[i] ) );
    }
  }

}
