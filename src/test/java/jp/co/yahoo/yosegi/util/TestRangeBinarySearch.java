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

package jp.co.yahoo.yosegi.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;

public class TestRangeBinarySearch {

  @Test
  public void T_createNewInstace_void() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
  }

  @Test
  public void T_addAndGet_equalsAddedObject_withStartIndexIsZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test0" , 0 );
    assertEquals( bs.get(0) , "test0" );
  }

  @Test
  public void T_addAndGet_equalsAddedObject_withStartIndexIsMoreThanZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test5" , 5 );
    assertEquals( bs.get(5) , "test5" );
  }

  @Test
  public void T_addAndGet_equalsAddedSameObjects_withStartIndexIsZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test0" , 0 );
    bs.add( "test1" , 1 );
    bs.add( "test2" , 2 );
    bs.add( "test3" , 3 );
    bs.add( "test4" , 4 );
    bs.add( "test5" , 5 );
    assertEquals( bs.get(0) , "test0" );
    assertEquals( bs.get(1) , "test1" );
    assertEquals( bs.get(2) , "test2" );
    assertEquals( bs.get(3) , "test3" );
    assertEquals( bs.get(4) , "test4" );
    assertEquals( bs.get(5) , "test5" );
    assertEquals( bs.get(6) , null );
  }

  @Test
  public void T_addAndGet_indexNotSetIsNull_withStartIndexIsMoreThanZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test5" , 5 );
    bs.add( "test6" , 6 );
    assertEquals( bs.get(0) , null );
    assertEquals( bs.get(1) , null );
    assertEquals( bs.get(2) , null );
    assertEquals( bs.get(3) , null );
    assertEquals( bs.get(4) , null );
    assertEquals( bs.get(5) , "test5" );
    assertEquals( bs.get(6) , "test6" );
    assertEquals( bs.get(7) , null );
  }

  @Test
  public void T_addAndGet_indexNotSetIsNull_withNullExistsInBetween() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test0" , 0 );
    bs.add( "test5" , 5 );
    assertEquals( bs.get(0) , "test0" );
    assertEquals( bs.get(1) , null );
    assertEquals( bs.get(2) , null );
    assertEquals( bs.get(3) , null );
    assertEquals( bs.get(4) , null );
    assertEquals( bs.get(5) , "test5" );
    assertEquals( bs.get(6) , null );
  }

  @Test
  public void T_add_throwsException_withIndexIsLessThanZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    assertThrows( RuntimeException.class ,
      () -> {
        bs.add( "test0" , -1 );
      }
    );
  }

  @Test
  public void T_add_throwsException_withExistingIndex() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test0" , 0 );
    assertThrows( RuntimeException.class ,
      () -> {
        bs.add( "test0" , 0 );
      }
    );
  }

  @Test
  public void T_add_throwsException_withLessThanCurrentIndex() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test0" , 1 );
    assertThrows( RuntimeException.class ,
      () -> {
        bs.add( "test0" , 0 );
      }
    );
  }

  @Test
  public void T_size_numberAdded_withStartIndexIsZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    assertEquals( bs.size() , 0 );
    bs.add( "test0" , 0 );
    assertEquals( bs.size() , 1 );
    bs.add( "test1" , 1 );
    assertEquals( bs.size() , 2 );
  }

  @Test
  public void T_size_numberAddedAndNumberNull_withStartIndexIsMoreThanZero() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    assertEquals( bs.size() , 0 );
    bs.add( "test5" , 5 );
    assertEquals( bs.size() , 6 );
    bs.add( "test6" , 6 );
    assertEquals( bs.size() , 7 );
  }

  @Test
  public void T_size_numberAddedAndNumberNull_whenSkipIndex() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    assertEquals( bs.size() , 0 );
    bs.add( "test0" , 0 );
    assertEquals( bs.size() , 1 );
    bs.add( "test5" , 5 );
    assertEquals( bs.size() , 6 );
  }

  @Test
  public void T_clear_void() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    assertEquals( bs.size() , 0 );
    bs.add( "test0" , 0 );
    assertEquals( bs.size() , 1 );
    bs.clear();
    assertEquals( bs.size() , 0 );
  }

  @Test
  public void T_getIndexAndObjectList_addedObject() {
    RangeBinarySearch<String> bs = new RangeBinarySearch<String>();
    bs.add( "test0" , 0 );
    bs.add( "test5" , 5 );

    List<IndexAndObject<String>> indexAndObjList = bs.getIndexAndObjectList();
    assertEquals( 2 , indexAndObjList.size() );
    IndexAndObject<String> list1 = indexAndObjList.get(0);
    assertEquals( list1.get(0) , "test0" );

    IndexAndObject<String> list2 = indexAndObjList.get(1);
    assertEquals( list2.get(5) , "test5" );
  }

}
