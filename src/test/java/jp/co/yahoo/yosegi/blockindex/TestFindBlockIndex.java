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
package jp.co.yahoo.yosegi.blockindex;

import java.io.IOException;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.filter.NumberFilter;
import jp.co.yahoo.yosegi.spread.column.filter.NumberRangeFilter;
import jp.co.yahoo.yosegi.spread.column.filter.NumberFilterType;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

public class TestFindBlockIndex{

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.blockindex.ByteRangeBlockIndex" , ByteRangeBlockIndex.class ),
      arguments( "jp.co.yahoo.yosegi.blockindex.ShortRangeBlockIndex" , ShortRangeBlockIndex.class ),
      arguments( "jp.co.yahoo.yosegi.blockindex.IntegerRangeBlockIndex" , IntegerRangeBlockIndex.class ),
      arguments( "jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex" , LongRangeBlockIndex.class ),
      arguments( "jp.co.yahoo.yosegi.blockindex.FloatRangeBlockIndex" , FloatRangeBlockIndex.class ),
      arguments( "jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex" , DoubleRangeBlockIndex.class ),
      arguments( "jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex" , StringRangeBlockIndex.class )
    );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_get_1( final String sc , final Class c ) throws IOException{
    IBlockIndex obj = FindBlockIndex.get( sc );
    assertEquals( obj.getClass().getName() ,  c.getName() );
  }

  @Test
  public void T_get_2() throws IOException{
    assertThrows( IOException.class ,
      () -> {
        FindBlockIndex.get( null );
      }
    );
  }

  @Test
  public void T_get_3() throws IOException{
    assertThrows( IOException.class ,
      () -> {
        FindBlockIndex.get( "" );
      }
    );
  }

  @Test
  public void T_get_4() throws IOException{
    assertThrows( IOException.class ,
      () -> {
        FindBlockIndex.get( "java.lang.String" );
      }
    );
  }

  @Test
  public void T_get_5() throws IOException{
    assertThrows( IOException.class ,
      () -> {
        FindBlockIndex.get( "___HOGEHOGE__" );
      }
    );
  }

}
