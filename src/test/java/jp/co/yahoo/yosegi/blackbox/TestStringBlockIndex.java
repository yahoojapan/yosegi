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

public class TestStringBlockIndex{

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker" ) ),
      arguments( createStringTestData( "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker" ) )
    );
  }

  public static IBlockIndex createStringTestData( final String targetClassName ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "y" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 1 );

    IColumn column2 = new PrimitiveColumn( ColumnType.STRING , "column" );
    column2.add( ColumnType.STRING , new StringObj( "bb" ) , 0 );
    column2.add( ColumnType.STRING , new StringObj( "bb" ) , 1 );

    IColumn column3 = new PrimitiveColumn( ColumnType.STRING , "column" );
    column3.add( ColumnType.STRING , new StringObj( "y" ) , 0 );
    column3.add( ColumnType.STRING , new StringObj( "y" ) , 1 );

    IColumn column4 = new PrimitiveColumn( ColumnType.STRING , "column" );
    column4.add( ColumnType.STRING , new StringObj( "x" ) , 0 );
    column4.add( ColumnType.STRING , new StringObj( "c" ) , 1 );

    IColumn column5 = new PrimitiveColumn( ColumnType.STRING , "column" );
    column5.add( ColumnType.STRING , new StringObj( "b" ) , 0 );
    column5.add( ColumnType.STRING , new StringObj( "b" ) , 1 );

    BlockIndexNode node = new BlockIndexNode();

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    ColumnBinary c1 = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    ColumnBinary c2 = maker.toBinary( defaultConfig , null , new CompressResultNode() , column2 );
    ColumnBinary c3 = maker.toBinary( defaultConfig , null , new CompressResultNode() , column3 );
    ColumnBinary c4 = maker.toBinary( defaultConfig , null , new CompressResultNode() , column4 );
    ColumnBinary c5 = maker.toBinary( defaultConfig , null , new CompressResultNode() , column5 );

    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c1 , 0 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c2 , 1 );
    FindColumnBinaryMaker.get( c3.makerClassName ).setBlockIndexNode( node , c3 , 2 );
    FindColumnBinaryMaker.get( c4.makerClassName ).setBlockIndexNode( node , c4 , 3 );
    FindColumnBinaryMaker.get( c5.makerClassName ).setBlockIndexNode( node , c5 , 4 );

    return node.getChildNode( "column" ).getBlockIndex();
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_perfectMatch_1( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 4 };
    IFilter filter = new PerfectMatchStringFilter( "b" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_perfectMatch_2( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 2 };
    IFilter filter = new PerfectMatchStringFilter( "y" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_perfectMatch_3( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 3 };
    IFilter filter = new PerfectMatchStringFilter( "d" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_forwardMatch_1( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 4 };
    IFilter filter = new ForwardMatchStringFilter( "b" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_forwardMatch_2( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 };
    IFilter filter = new ForwardMatchStringFilter( "bb" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_forwardMatch_3( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 3 };
    IFilter filter = new ForwardMatchStringFilter( "d" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_forwardMatch_4( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 3 };
    IFilter filter = new ForwardMatchStringFilter( "x" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_forwardMatch_5( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 };
    IFilter filter = new ForwardMatchStringFilter( "xx" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_vackwardMatch_5( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 };
    IFilter filter = new BackwardMatchStringFilter( "a" );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
    assertNull( resultIndexList );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_compareString_1( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 3 };
    IFilter filter = new RangeStringCompareFilter( "bb" , true , "x" , true );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_2( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 3 };
    IFilter filter = new RangeStringCompareFilter( "bb" , false , "x" , true );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_3( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 3 };
    IFilter filter = new RangeStringCompareFilter( "bb" , true , "x" , false );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_4( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 3 };
    IFilter filter = new RangeStringCompareFilter( "bb" , false , "x" , false );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_5( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 2 , 3 , 4 };
    IFilter filter = new RangeStringCompareFilter( "bb" , true , "x" , true , true );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_6( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 };
    IFilter filter = new RangeStringCompareFilter( "bb" , false , "x" , true , true );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_7( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 2 , 3 , 4 };
    IFilter filter = new RangeStringCompareFilter( "bb" , true , "x" , false , true );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_compareString_8( final IBlockIndex index ) throws IOException{
    int[] mustReadIndex = { 0 , 1 , 2 , 3 , 4 };
    IFilter filter = new RangeStringCompareFilter( "bb" , false , "x" , false , true );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_dictionaryString_1( final IBlockIndex index ) throws IOException{
    Set<String> d = new HashSet<String>();
    d.add( "e" );
    d.add( "b" );
    int[] mustReadIndex = { 0 , 1 , 3 , 4 };
    IFilter filter = new StringDictionaryFilter( d );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
  public void T_dictionaryString_2( final IBlockIndex index ) throws IOException{
    Set<String> d = new HashSet<String>();
    d.add( "x" );
    d.add( "y" );
    int[] mustReadIndex = { 0 , 2 , 3 };
    IFilter filter = new StringDictionaryFilter( d );
    List<Integer> resultIndexList = index.getBlockSpreadIndex( filter );
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
