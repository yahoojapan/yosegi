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

public class TestShortRangeBlockIndex{

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)21 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)9 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)15 ) ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)10 ) ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)20 ) ) , false ),

      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LT , new ShortObj( (short)9 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LT , new ShortObj( (short)10 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LT , new ShortObj( (short)11 ) ) , false ),

      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LE , new ShortObj( (short)9 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LE , new ShortObj( (short)10 ) ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LE , new ShortObj( (short)11 ) ) , false ),

      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GT , new ShortObj( (short)21 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GT , new ShortObj( (short)20 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GT , new ShortObj( (short)19 ) ) , false ),

      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GE , new ShortObj( (short)21 ) ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GE , new ShortObj( (short)20 ) ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GE , new ShortObj( (short)19 ) ) , false ),

      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)5 ) , true , new ShortObj( (short)15 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , true , new ShortObj( (short)21 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)15 ) , true , new ShortObj( (short)25 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)20 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , true , new ShortObj( (short)20 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)15 ) , true , new ShortObj( (short)16 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , true , new ShortObj( (short)9 ) , true ) , true ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)21 ) , true , new ShortObj( (short)21 ) , true ) , true ),

      // 10 <= c < 11
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)11 ) , false ) , false ),
      // 5 <= c < 16
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)5 ) , true , new ShortObj( (short)16 ) , false ) , false ),
      // 20 <= c < 22
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , true , new ShortObj( (short)22 ) , false ) , false ),
      // 15 <= c < 26
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)15 ) , true , new ShortObj( (short)26 ) , false ) , false ),
      // 10 <= c < 21
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)21 ) , false ) , false ),
      // 20 <= c < 21
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , true , new ShortObj( (short)21 ) , false ) , false ),
      // 15 <= c < 17
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)15 ) , true , new ShortObj( (short)17 ) , false ) , false ),
      // 9 <= c < 10
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , true , new ShortObj( (short)10 ) , false ) , true ),
      // 21 <= c < 22
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)21 ) , true , new ShortObj( (short)22 ) , false ) , true ),

      // 9 < c <= 10
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , false , new ShortObj( (short)10 ) , true ) , false ),
      // 4 < c <= 15
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)4 ) , false , new ShortObj( (short)15 ) , true ) , false ),
      // 19 < c <= 21
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)19 ) , false , new ShortObj( (short)21 ) , true ) , false ),
      // 14 < c <= 25
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)14 ) , false , new ShortObj( (short)25 ) , true ) , false ),
      // 9 < c <= 20
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , false , new ShortObj( (short)20 ) , true ) , false ),
      // 19 < c <= 20
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)19 ) , false , new ShortObj( (short)20 ) , true ) , false ),
      // 14 < c <= 16
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)14 ) , false , new ShortObj( (short)16 ) , true ) , false ),
      // 8 < c <= 9
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)8 ) , false , new ShortObj( (short)9 ) , true ) , true ),
      // 20 < c <= 21
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , false , new ShortObj( (short)21 ) , true ) , true ),

      // 9 < c < 11
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , false , new ShortObj( (short)11 ) , false ) , false ),
      // 4 < c < 16
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)4 ) , false , new ShortObj( (short)16 ) , false ) , false ),
      // 19 < c < 22
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)19 ) , false , new ShortObj( (short)22 ) , false ) , false ),
      // 14 < c < 26
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)14 ) , false , new ShortObj( (short)26 ) , false ) , false ),
      // 9 < c < 21
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , false , new ShortObj( (short)21 ) , false ) , false ),
      // 19 < c < 21
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)19 ) , false , new ShortObj( (short)21 ) , false ) , false ),
      // 14 < c < 17
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)14 ) , false , new ShortObj( (short)17 ) , false ) , false ),
      // 8 < c < 10
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)8 ) , false , new ShortObj( (short)10 ) , false ) , true ),
      // 20 < c < 22
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , false , new ShortObj( (short)22 ) , false ) , true ),

      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)5 ) , true , new ShortObj( (short)15 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)20 ) , true , new ShortObj( (short)21 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)15 ) , true , new ShortObj( (short)25 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)10 ) , true , new ShortObj( (short)20 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)20 ) , true , new ShortObj( (short)20 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)15 ) , true , new ShortObj( (short)16 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)9 ) , true , new ShortObj( (short)9 ) , true ) , false ),
      arguments( new ShortRangeBlockIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)21 ) , true , new ShortObj( (short)21 ) , true ) , false )
    );
  }

  @Test
  public void T_newInstance_1(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex();
    assertEquals( Short.MAX_VALUE , bIndex.getMin() );
    assertEquals( Short.MIN_VALUE , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new ShortRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_SHORT , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ShortRangeBlockIndex( (short)110 , (short)150 ) ) );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)150 , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ShortRangeBlockIndex( (short)9 , (short)20 ) ) );
    assertEquals( (short)9 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ShortRangeBlockIndex( (short)10 , (short)21 ) ) );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new ShortRangeBlockIndex( (short)9 , (short)21 ) ) );
    assertEquals( (short)9 , bIndex.getMin() );
    assertEquals( (short)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( (short)10 , bIndex.getMin() );
    assertEquals( (short)20 , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    assertEquals( 4 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    ShortRangeBlockIndex bIndex = new ShortRangeBlockIndex( (short)10 , (short)20 );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    ShortRangeBlockIndex bIndex2 = new ShortRangeBlockIndex();
    assertEquals( Short.MAX_VALUE , bIndex2.getMin() );
    assertEquals( Short.MIN_VALUE , bIndex2.getMax() );
    bIndex2.setFromBinary( binary , 0 , binary.length );
    assertEquals( bIndex2.getMin() , bIndex.getMin() );
    assertEquals( bIndex2.getMax() , bIndex.getMax() );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_canBlockSkip_1( final IBlockIndex bIndex , final IFilter filter , final boolean result ){
    if( result ){
      assertEquals( result , bIndex.getBlockSpreadIndex( filter ).isEmpty() );
    }
    else{
      assertTrue( bIndex.getBlockSpreadIndex( filter ) == null );
    }
  }

}
