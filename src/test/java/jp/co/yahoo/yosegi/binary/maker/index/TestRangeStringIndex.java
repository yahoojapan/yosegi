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

public class TestRangeStringIndex{

  private static final boolean[] dummy = new boolean[0];

  public static Stream<Arguments> data1() {
    return Stream.of(
      arguments( new RangeStringIndex( "10" , "20" ) , new PerfectMatchStringFilter( "21" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new PerfectMatchStringFilter( "09" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new PerfectMatchStringFilter( "15" ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new PerfectMatchStringFilter( "10" ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new PerfectMatchStringFilter( "20" ) , null ),

      arguments( new RangeStringIndex( "10" , "20" ) , new LtStringCompareFilter("09" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new LtStringCompareFilter( "10" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new LtStringCompareFilter( "11" ) , null ),

      arguments( new RangeStringIndex( "10" , "20" ) , new LeStringCompareFilter( "09" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new LeStringCompareFilter( "10" ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new LeStringCompareFilter( "11" ) , null ),

      arguments( new RangeStringIndex( "10" , "20" ) , new GtStringCompareFilter( "21" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new GtStringCompareFilter( "20" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new GtStringCompareFilter( "19" ) , null ),

      arguments( new RangeStringIndex( "10" , "20" ) , new GeStringCompareFilter( "21" ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new GeStringCompareFilter( "20" ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new GeStringCompareFilter( "19" ) , null ),

      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true ) , null ),
      arguments( new RangeStringIndex( "00" , "20" ) , new RangeStringCompareFilter( "05" , true , "15" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "21" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "25" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "20" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "20" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "16" , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "09" , true , "09" , true ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "21" , true , "21" , true ) , dummy ),

      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "05" , true , "15" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "21" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "25" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "20" , true , true ) , dummy ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "10" , true , "10" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "20" , true , "20" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "15" , true , "16" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "09" , true , "09" , true , true ) , null ),
      arguments( new RangeStringIndex( "10" , "20" ) , new RangeStringCompareFilter( "21" , true , "21" , true , true ) , null ),

      arguments( new RangeStringIndex( "a__0" , "j__9" ) , new RangeStringCompareFilter( "b" , true , "b__1" , true , true ) , null )
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
