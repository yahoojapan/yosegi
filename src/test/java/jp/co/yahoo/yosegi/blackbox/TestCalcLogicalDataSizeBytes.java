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

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.binary.*;
import jp.co.yahoo.yosegi.binary.maker.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.column.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestCalcLogicalDataSizeBytes {

  public static Stream<Arguments> data1() throws IOException {
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.binary.maker.OptimizedNullArrayDumpBytesColumnBinaryMaker" ) 
    );
  }

  public static ColumnBinary create( final IColumn column , final String targetClassName ) throws IOException {
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( targetClassName );
    return maker.toBinary( defaultConfig , null , new CompressResultNode() , column );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_notNull_1( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc0".getBytes() ) , 0 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 1 );
    column.add( ColumnType.STRING , new BytesObj( "abc2".getBytes() ) , 2 );
    column.add( ColumnType.STRING , new BytesObj( "abc3".getBytes() ) , 3 );
    column.add( ColumnType.STRING , new BytesObj( "abc4".getBytes() ) , 4 );
    column.add( ColumnType.STRING , new BytesObj( "abc5".getBytes() ) , 5 );
    column.add( ColumnType.STRING , new BytesObj( "abc6".getBytes() ) , 6 );
    column.add( ColumnType.STRING , new BytesObj( "abc7".getBytes() ) , 7 );
    column.add( ColumnType.STRING , new BytesObj( "abc8".getBytes() ) , 8 );
    column.add( ColumnType.STRING , new BytesObj( "abc9".getBytes() ) , 9 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 10 + 4 * 10 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_notNull_2( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 0 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 1 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 2 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 3 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 4 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 5 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 6 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 7 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 8 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 9 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 10 + 4 * 10 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_1( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 1 );
    column.add( ColumnType.STRING , new BytesObj( "abc2".getBytes() ) , 2 );
    column.add( ColumnType.STRING , new BytesObj( "abc4".getBytes() ) , 4 );
    column.add( ColumnType.STRING , new BytesObj( "abc5".getBytes() ) , 5 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 4 + 4 * 4 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_2( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 5 );
    column.add( ColumnType.STRING , new BytesObj( "abc2".getBytes() ) , 6 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 2 + 4 * 2 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_3( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 1 );
    column.add( ColumnType.STRING , new BytesObj( "abc2".getBytes() ) , 5 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 2 + 4 * 2 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_4( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 1 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 2 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 4 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 5 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 4 + 4 * 4 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_5( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 5 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 6 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 2 + 4 * 2 );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_hasNull_6( final String targetClassName ) throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column" );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 1 );
    column.add( ColumnType.STRING , new BytesObj( "abc1".getBytes() ) , 5 );

    ColumnBinary columnBinary = create( column , targetClassName );
    assertEquals( columnBinary.logicalDataSize , Integer.BYTES * 2 + 4 * 2 );
  }

}
