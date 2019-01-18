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
import java.nio.IntBuffer;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.maker.IDicManager;
import jp.co.yahoo.yosegi.spread.column.*;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.expression.*;

import jp.co.yahoo.yosegi.message.objects.*;

import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;

public class TestRangeShortIndex{

  private static final boolean[] dummy = new boolean[0];

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)21 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)9 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)15 ) ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)10 ) ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.EQUAL , new ShortObj( (short)20 ) ) , null ),

      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LT , new ShortObj( (short)9 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LT , new ShortObj( (short)10 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LT , new ShortObj( (short)11 ) ) , null ),

      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LE , new ShortObj( (short)9 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LE , new ShortObj( (short)10 ) ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.LE , new ShortObj( (short)11 ) ) , null ),

      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GT , new ShortObj( (short)21 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GT , new ShortObj( (short)20 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GT , new ShortObj( (short)19 ) ) , null ),

      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GE , new ShortObj( (short)21 ) ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GE , new ShortObj( (short)20 ) ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberFilter( NumberFilterType.GE , new ShortObj( (short)19 ) ) , null ),

      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)5 ) , true , new ShortObj( (short)15 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , true , new ShortObj( (short)21 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)15 ) , true , new ShortObj( (short)25 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)20 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)20 ) , true , new ShortObj( (short)20 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)15 ) , true , new ShortObj( (short)16 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)9 ) , true , new ShortObj( (short)9 ) , true ) , dummy ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( false , new ShortObj( (short)21 ) , true , new ShortObj( (short)21 ) , true ) , dummy ),

      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)5 ) , true , new ShortObj( (short)15 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)20 ) , true , new ShortObj( (short)21 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)15 ) , true , new ShortObj( (short)25 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)10 ) , true , new ShortObj( (short)20 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)10 ) , true , new ShortObj( (short)10 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)20 ) , true , new ShortObj( (short)20 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)15 ) , true , new ShortObj( (short)16 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)9 ) , true , new ShortObj( (short)9 ) , true ) , null ),
      arguments( new RangeShortIndex( (short)10 , (short)20 ) , new NumberRangeFilter( true , new ShortObj( (short)21 ) , true , new ShortObj( (short)21 ) , true ) , null )

    );
  }

  @ParameterizedTest
  @MethodSource( "data1" )
  public void T_filter_1( final ICellIndex cIndex , final IFilter filter , final boolean[] result ) throws IOException{
    boolean[] r = cIndex.filter( filter , new boolean[0] );
    if( r == null ){
      assertNull( result );
    }
    else{
      assertEquals( result.length , 0 );
    }
  }

}
