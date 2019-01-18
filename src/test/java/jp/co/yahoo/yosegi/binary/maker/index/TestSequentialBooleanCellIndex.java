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
package jp.co.yahoo.yosegi.binary.maker.index;

import java.io.IOException;

import java.util.List;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.spread.column.filter.BooleanFilter;
import jp.co.yahoo.yosegi.spread.column.filter.NullFilter;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestSequentialBooleanCellIndex{

  @Test
  public void T_newInstance_1(){
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( new byte[0] );
  }

  @Test
  public void T_filter_1() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    boolean[] result = index.filter( new BooleanFilter( true ) , new boolean[10] );
    assertEquals( false , result[0] );
    assertEquals( true , result[1] );
    assertEquals( false , result[2] );
    assertEquals( true , result[3] );
    assertEquals( false , result[4] );
    assertEquals( true , result[5] );
    assertEquals( false , result[6] );
    assertEquals( true , result[7] );
    assertEquals( false , result[8] );
    assertEquals( true , result[9] );
  }

  @Test
  public void T_filter_2() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    boolean[] result = index.filter( new BooleanFilter( false ) , new boolean[10] );
    assertEquals( true , result[0] );
    assertEquals( false , result[1] );
    assertEquals( true , result[2] );
    assertEquals( false , result[3] );
    assertEquals( true , result[4] );
    assertEquals( false , result[5] );
    assertEquals( true , result[6] );
    assertEquals( false , result[7] );
    assertEquals( true , result[8] );
    assertEquals( false , result[9] );
  }

  @Test
  public void T_filter_3() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    boolean[] result = index.filter( null , new boolean[10] );
    assertEquals( result , null );
  }

  @Test
  public void T_filter_4() throws IOException{
    byte[] data = new byte[10];
    for( int i = 0 ; i < 10 ; i++ ){
      data[i] = (byte)( i % 2 );
    }
    SequentialBooleanCellIndex index = new SequentialBooleanCellIndex( data );
    boolean[] result = index.filter( new NullFilter( ColumnType.BOOLEAN ) , new boolean[10] );
    assertEquals( result , null );
  }

}
