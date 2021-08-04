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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestBlockIndexNameShortCut {

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( "jp.co.yahoo.yosegi.blockindex.ByteRangeBlockIndex" , "R0" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.ShortRangeBlockIndex" , "R1" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.IntegerRangeBlockIndex" , "R2" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex" , "R3" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.FloatRangeBlockIndex" , "R4" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex" , "R5" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex" , "R6" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.FullRangeBlockIndex" , "FR0" ),
      arguments( "jp.co.yahoo.yosegi.blockindex.BooleanBlockIndex" , "BI0" )
    );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_get_1( final String c , final String sc ) throws IOException{
    assertEquals( sc , BlockIndexNameShortCut.getShortCutName( c ) );
    assertEquals( c , BlockIndexNameShortCut.getClassName( sc ) );
  }

}
