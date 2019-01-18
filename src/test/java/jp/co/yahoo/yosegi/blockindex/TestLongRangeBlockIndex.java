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

public class TestLongRangeBlockIndex{

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.EQUAL , new LongObj( (long)21 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.EQUAL , new LongObj( (long)9 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.EQUAL , new LongObj( (long)15 ) ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.EQUAL , new LongObj( (long)10 ) ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.EQUAL , new LongObj( (long)20 ) ) , false ),

      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.LT , new LongObj( (long)9 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.LT , new LongObj( (long)10 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.LT , new LongObj( (long)11 ) ) , false ),

      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.LE , new LongObj( (long)9 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.LE , new LongObj( (long)10 ) ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.LE , new LongObj( (long)11 ) ) , false ),

      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.GT , new LongObj( (long)21 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.GT , new LongObj( (long)20 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.GT , new LongObj( (long)19 ) ) , false ),

      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.GE , new LongObj( (long)21 ) ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.GE , new LongObj( (long)20 ) ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberFilter( NumberFilterType.GE , new LongObj( (long)19 ) ) , false ),

      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)10 ) , true , new LongObj( (long)10 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)5 ) , true , new LongObj( (long)15 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)20 ) , true , new LongObj( (long)21 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)15 ) , true , new LongObj( (long)25 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)10 ) , true , new LongObj( (long)20 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)10 ) , true , new LongObj( (long)10 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)20 ) , true , new LongObj( (long)20 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)15 ) , true , new LongObj( (long)16 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)9 ) , true , new LongObj( (long)9 ) , true ) , true ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( false , new LongObj( (long)21 ) , true , new LongObj( (long)21 ) , true ) , true ),

      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)10 ) , true , new LongObj( (long)10 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)5 ) , true , new LongObj( (long)15 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)20 ) , true , new LongObj( (long)21 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)15 ) , true , new LongObj( (long)25 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)10 ) , true , new LongObj( (long)20 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)10 ) , true , new LongObj( (long)10 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)20 ) , true , new LongObj( (long)20 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)15 ) , true , new LongObj( (long)16 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)9 ) , true , new LongObj( (long)9 ) , true ) , false ),
      arguments( new LongRangeBlockIndex( (long)10 , (long)20 ) , new NumberRangeFilter( true , new LongObj( (long)21 ) , true , new LongObj( (long)21 ) , true ) , false )

    );
  }

  @Test
  public void T_newInstance_1(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
  }

  @Test
  public void T_newInstance_2(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex();
    assertEquals( Long.MAX_VALUE , bIndex.getMin() );
    assertEquals( Long.MIN_VALUE , bIndex.getMax() );
  }

  @Test
  public void T_getBlockIndexType_1(){
    IBlockIndex bIndex = new LongRangeBlockIndex();
    assertEquals( BlockIndexType.RANGE_LONG , bIndex.getBlockIndexType() );
  }

  @Test
  public void T_merge_1(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new LongRangeBlockIndex( (long)110 , (long)150 ) ) );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)150 , bIndex.getMax() );
  }

  @Test
  public void T_merge_2(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new LongRangeBlockIndex( (long)9 , (long)20 ) ) );
    assertEquals( (long)9 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
  }

  @Test
  public void T_merge_3(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new LongRangeBlockIndex( (long)10 , (long)21 ) ) );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_4(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
    assertTrue( bIndex.merge( new LongRangeBlockIndex( (long)9 , (long)21 ) ) );
    assertEquals( (long)9 , bIndex.getMin() );
    assertEquals( (long)21 , bIndex.getMax() );
  }

  @Test
  public void T_merge_5(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( (long)10 , bIndex.getMin() );
    assertEquals( (long)20 , bIndex.getMax() );
    assertFalse( bIndex.merge( UnsupportedBlockIndex.INSTANCE ) );
  }

  @Test
  public void T_getBinarySize_1(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    assertEquals( Long.BYTES * 2 , bIndex.getBinarySize() );
  }

  @Test
  public void T_binary_1(){
    LongRangeBlockIndex bIndex = new LongRangeBlockIndex( (long)10 , (long)20 );
    byte[] binary = bIndex.toBinary();
    assertEquals( binary.length , bIndex.getBinarySize() );
    LongRangeBlockIndex bIndex2 = new LongRangeBlockIndex();
    assertEquals( Long.MAX_VALUE , bIndex2.getMin() );
    assertEquals( Long.MIN_VALUE , bIndex2.getMax() );
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
