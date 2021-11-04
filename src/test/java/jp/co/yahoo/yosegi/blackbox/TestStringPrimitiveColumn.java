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

import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
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

public class TestStringPrimitiveColumn {

  public static Stream<Arguments> data1() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker" )
    );
  }

  public static Stream<Arguments> stringColumnBinaryMaker() throws IOException{
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.RleStringColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.DictionaryRleStringColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpStringColumnBinaryMaker" ),
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayStringColumnBinaryMaker" )
    );
  }

  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    return toColumn(columnBinary, null);
  }

  public IColumn toColumn(final ColumnBinary columnBinary, Integer loadCount) throws IOException {
    if (loadCount == null) {
      loadCount = (columnBinary.isSetLoadSize) ? columnBinary.loadSize : columnBinary.rowCount;
    }
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  public IColumn createNotNullColumn( final String targetClassName ) throws IOException{
    return createNotNullColumn( targetClassName , null , null );
  }

  public IColumn createNotNullColumn( final String targetClassName , final int[] repetitions , final Integer loadSize ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );
    column.add( ColumnType.STRING , new StringObj( "" ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if ( repetitions == null ) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions( repetitions , loadSize );
      return toColumn(columnBinary);
    }
  }

  public IColumn createNullColumn( final String targetClassName ) throws IOException{
    return createNullColumn( targetClassName , null , null );
  }

  public IColumn createNullColumn( final String targetClassName , final int[] repetitions , final Integer loadSize ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if ( repetitions == null ) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions( repetitions , loadSize );
      return toColumn(columnBinary);
    }
  }

  public IColumn createHasNullColumn( final String targetClassName ) throws IOException{
    return createHasNullColumn( targetClassName , null , null );
  }

  public IColumn createHasNullColumn( final String targetClassName , final int[] repetitions , final Integer loadSize ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 8 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if ( repetitions == null ) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions( repetitions , loadSize );
      return toColumn(columnBinary);
    }
  }

  public IColumn createLastCellColumn( final String targetClassName ) throws IOException{
    return createLastCellColumn( targetClassName , null , null );
  }

  public IColumn createLastCellColumn( final String targetClassName , final int[] repetitions , final Integer loadSize ) throws IOException{
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 10 );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    if ( repetitions == null ) {
      return toColumn(columnBinary, loadSize);
    } else {
      columnBinary.setRepetitions( repetitions , loadSize );
      return toColumn(columnBinary);
    }
  }

  public IColumn createNullExistsContinuouslyColumn( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    for ( int i = 0 ; i < 1000000 ; i++ ) {
      if ( ( i % 100 ) < 10 ) {
        column.add( ColumnType.STRING , new StringObj( Integer.toString( i ) ) , i );
      }
    }

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    ColumnBinary columnBinary = maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
    return toColumn(columnBinary);
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_notNull_1( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withAllValueIndex( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,1,1,1,1,1} , 11);
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withLargeLoadIndex( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1} , 16 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "" );
    for ( int i = 11 ; i < 16 ; i++ ) {
      assertNull( column.get(i).getRow() );
    }
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withLoadIndexIsHead5( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,0,0,0,0,0} , 6 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "bc" );
    for ( int i = 6 ; i < 11 ; i++ ) {
      assertNull( column.get(i).getRow() );
    }
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withLoadIndexTail5( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{0,0,0,0,0,0,1,1,1,1,1} , 5 );
    assertEquals( column.size() , 5 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withOddNumberIndex( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{0,1,0,1,0,1,0,1,0,1,0} , 5 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "cd" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withAllValueIndexAndExpand( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,2,1,2,1,2} , 17 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(11).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(12).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(13).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(14).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(15).getRow() ) ).getString() , "" );
    assertEquals( ( (PrimitiveObject)( column.get(16).getRow() ) ).getString() , "" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withLargeLoadIndexAndExpand( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1} , 24 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "bc" );
    assertEquals( ( (PrimitiveObject)( column.get(9).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(11).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(12).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(13).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(14).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(15).getRow() ) ).getString() , "" );
    assertEquals( ( (PrimitiveObject)( column.get(16).getRow() ) ).getString() , "" );
    for ( int i = 17 ; i < 24 ; i++ ) {
      assertNull( column.get(i).getRow() );
    }
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withLoadIndexIsHead5AndExpand( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,0,0,0,0,0} , 9 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "ab" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "abc" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "abcd" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "bc" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadFromNotNullColumn_equals_withLoadIndexTail5AndExpand( final String targetClassName ) throws IOException{
    IColumn column = createNotNullColumn(
        targetClassName ,
        new int[]{0,0,0,0,0,0,2,1,2,1,2} , 8 );
    assertEquals( column.size() , 8 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "bcd" );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "bcde" );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(5).getRow() ) ).getString() , "cd" );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "" );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_null_1( final String targetClassName ) throws IOException{
    IColumn column = createNullColumn( targetClassName );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadNotNullColumn_equals_withAllValueIndexAndExpand( final String targetClassName ) throws IOException{
    IColumn column = createNullColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,2,1,2,1,2} , 16 );
    for ( int i = 0 ; i < 16 ; i++ ) {
      assertNull( column.get(i).getRow() );
    }
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_1( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn( targetClassName );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadHasNullColumn_equals_withAllValueIndex( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,1,1,1,1,1} , 11 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(8).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadHasNullColumn_equals_withLoadIndexIsHead5( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,0,0,0,0,0} , 6 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "b" );
    assertNull( column.get(5).getRow() );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadHasNullColumn_equals_withLoadIndexTail5( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn(
        targetClassName ,
        new int[]{0,0,0,0,0,0,1,1,1,1,1} , 5 );
    assertEquals( column.size() , 5 );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(2).getRow() ) ).getString() , "c" );
    assertNull( column.get(3).getRow() );
    assertNull( column.get(4).getRow() );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadHasNullColumn_equals_withAllValueIndexAndExpand( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,2,1,2,1,2} , 17 );
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "a" );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertNull( column.get(4).getRow() );
    assertNull( column.get(5).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "b" );
    assertNull( column.get(8).getRow() );
    assertNull( column.get(9).getRow() );
    assertNull( column.get(10).getRow() );
    assertNull( column.get(11).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(12).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(13).getRow() ) ).getString() , "c" );
    assertNull( column.get(14).getRow() );
    assertNull( column.get(15).getRow() );
    assertNull( column.get(16).getRow() );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadHasNullColumn_equals_withLoadIndexIsHead5AndExpand( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,0,0,0,0,0} , 9);
    assertEquals( ( (PrimitiveObject)( column.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( column.get(1).getRow() ) ).getString() , "a" );
    assertNull( column.get(2).getRow() );
    assertNull( column.get(3).getRow() );
    assertNull( column.get(4).getRow() );
    assertNull( column.get(5).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "b" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "b" );
    assertNull( column.get(8).getRow() );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadHasNullColumn_equals_withLoadIndexTail5AndExpand( final String targetClassName ) throws IOException{
    IColumn column = createHasNullColumn(
        targetClassName ,
        new int[]{0,0,0,0,0,0,2,1,2,1,2} , 8 );
    assertEquals( column.size() , 8 );
    assertNull( column.get(0).getRow() );
    assertNull( column.get(1).getRow() );
    assertNull( column.get(2).getRow() );
    assertEquals( ( (PrimitiveObject)( column.get(3).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "c" );
    assertNull( column.get(5).getRow() );
    assertNull( column.get(6).getRow() );
    assertNull( column.get(7).getRow() );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_lastCell_1( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn( targetClassName );
    for( int i = 0 ; i < 10 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadLastCellColumn_equals_withAllValueIndex( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,1,1,1,1,1} , 11 );
    for( int i = 0 ; i < 10 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(10).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadLastCellColumn_equals_withLoadIndexIsHead5( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn(
        targetClassName ,
        new int[]{1,1,1,1,1,1,0,0,0,0,0} , 6 );
    for( int i = 0 ; i < 6 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadLastCellColumn_equals_withLoadIndexTail5( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn(
        targetClassName ,
        new int[]{0,0,0,0,0,0,1,1,1,1,1} , 5 );
    assertEquals( column.size() , 5 );
    for( int i = 0 ; i < 4 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(4).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadLastCellColumn_equals_withAllValueIndexAndExpand( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,2,1,2,1,2} , 17 );
    for( int i = 0 ; i < 15 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(15).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(16).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadLastCellColumn_equals_withLoadIndexIsHead5AndExpand( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn(
        targetClassName ,
        new int[]{2,1,2,1,2,1,0,0,0,0,0} , 9 );
    for( int i = 0 ; i < 9 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_loadLastCellColumn_equals_withLoadIndexTail5AndExpand( final String targetClassName ) throws IOException{
    IColumn column = createLastCellColumn(
        targetClassName ,
        new int[]{0,0,0,0,0,0,2,1,2,1,2} , 8 );
    assertEquals( column.size() , 8 );
    for( int i = 0 ; i < 6 ; i++ ){
      assertNull( column.get(i).getRow() );
    }
    assertEquals( ( (PrimitiveObject)( column.get(6).getRow() ) ).getString() , "c" );
    assertEquals( ( (PrimitiveObject)( column.get(7).getRow() ) ).getString() , "c" );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_get_equalsSetValue_whenNullExistsContinuously( final String targetClassName ) throws IOException{
    IColumn column = createNullExistsContinuouslyColumn( targetClassName );
    for ( int i = 0 ; i < 1000000 ; i++ ) {
      if ( ( i % 100 ) < 10 ) {
        assertEquals( ( (PrimitiveObject)( column.get(i).getRow() ) ).getString() , Integer.toString(i) );
      }
      else {
        assertNull( column.get(i).getRow() );
      }
    }
  }

  @ParameterizedTest
  @MethodSource( "stringColumnBinaryMaker" )
  public void T_load_exception_withLessThan0( final String targetClassName ) throws IOException{
    assertThrows( IOException.class ,
        () -> {
          IColumn column = createLastCellColumn(
            targetClassName ,
            new int[]{-1,0,1,2} , 3);
        }
    );
  }

  @ParameterizedTest
  @MethodSource("stringColumnBinaryMaker")
  public void T_load_exception_withLessThan0_withOutOfBoundsLoadIndexAndExpand(
      final String targetClassName) {
    assertThrows(
        IOException.class,
        () -> {
          IColumn column =
              createLastCellColumn(
                  targetClassName, new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1}, 3);
        });
  }
}
