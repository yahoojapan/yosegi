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

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestCollectionUtils {

  @Test
  public void T_unionFromSortedCollection_1(){
    List<Integer> a = new ArrayList<Integer>();
    a.add( 0 );
    a.add( 6 );

    List<Integer> b = new ArrayList<Integer>();
    b.add( 1 );
    b.add( 2 );
    b.add( 6 );

    List<Integer> result = CollectionUtils.unionFromSortedCollection( a , b );
    assertEquals( result.get(0).intValue() , 0 );
    assertEquals( result.get(1).intValue() , 1 );
    assertEquals( result.get(2).intValue() , 2 );
    assertEquals( result.get(3).intValue() , 6 );
  }

  @Test
  public void T_unionFromSortedCollection_2(){
    List<Integer> a = new ArrayList<Integer>();
    a.add( 0 );
    a.add( 6 );

    List<Integer> b = new ArrayList<Integer>();

    List<Integer> result = CollectionUtils.unionFromSortedCollection( a , b );
    assertEquals( result.get(0).intValue() , 0 );
    assertEquals( result.get(1).intValue() , 6 );
  }

  @Test
  public void T_unionFromSortedCollection_3(){
    List<Integer> a = new ArrayList<Integer>();
    a.add( 0 );
    a.add( 6 );

    List<Integer> b = new ArrayList<Integer>();
    b.add( 1 );
    b.add( 2 );
    b.add( 6 );

    List<Integer> result = CollectionUtils.unionFromSortedCollection( b , a );
    assertEquals( result.get(0).intValue() , 0 );
    assertEquals( result.get(1).intValue() , 1 );
    assertEquals( result.get(2).intValue() , 2 );
    assertEquals( result.get(3).intValue() , 6 );
  }

  @Test
  public void T_unionFromSortedCollection_4(){
    List<Integer> a = new ArrayList<Integer>();
    a.add( 0 );
    a.add( 6 );

    List<Integer> b = new ArrayList<Integer>();

    List<Integer> result = CollectionUtils.unionFromSortedCollection( b , a );
    assertEquals( result.get(0).intValue() , 0 );
    assertEquals( result.get(1).intValue() , 6 );
  }

  @Test
  public void T_unionFromSortedCollection_5(){
    List<Integer> a = new ArrayList<Integer>();
    a.add( 2 );

    List<Integer> b = new ArrayList<Integer>();
    b.add( 5 );

    List<Integer> result = CollectionUtils.unionFromSortedCollection( b , a );
    assertEquals( result.get(0).intValue() , 2 );
    assertEquals( result.get(1).intValue() , 5 );
  }

  @Test
  public void T_unionFromSortedCollection_6(){
    List<Integer> a = new ArrayList<Integer>();
    a.add( 0 );

    List<Integer> b = new ArrayList<Integer>();
    b.add( 2 );

    List<Integer> result = CollectionUtils.unionFromSortedCollection( a , b );
    assertEquals( result.get(0).intValue() , 0 );
    assertEquals( result.get(1).intValue() , 2 );
  }

}
