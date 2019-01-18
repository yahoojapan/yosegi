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
package jp.co.yahoo.yosegi.spread.column.filter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestRangeStringCompareFilter {

  @Test
  public void T_1(){
    RangeStringCompareFilter filter = new RangeStringCompareFilter( "a" , true , "b" , true , false );
    IStringComparator comp = filter.getStringComparator();
    assertEquals( comp.isFilterString( "a1" ) , false );
    assertEquals( comp.isFilterString( "b" ) , false );
    assertEquals( comp.isFilterString( "a" ) , false );
    assertEquals( comp.isFilterString( "aa" ) , false );
    assertEquals( comp.isFilterString( "a " ) , false );
    assertEquals( comp.isFilterString( "9" ) , true );
    assertEquals( comp.isFilterString( "b0" ) , true );
  }

  @Test
  public void T_2(){
    RangeStringCompareFilter filter = new RangeStringCompareFilter( "2001-01-12" , true , "2001-02-11" , true , false );
    IStringComparator comp = filter.getStringComparator();
    assertEquals( comp.isFilterString( "2001-01-11" ) , true );
    assertEquals( comp.isFilterString( "2001-01-12" ) , false );
    assertEquals( comp.isFilterString( "2001-02-01" ) , false );
    assertEquals( comp.isFilterString( "2001-02-11" ) , false );
    assertEquals( comp.isFilterString( "2001-02-12" ) , true );
  }


}
