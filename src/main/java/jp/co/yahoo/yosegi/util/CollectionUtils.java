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

import java.util.ArrayList;
import java.util.List;

public final class CollectionUtils {

  private CollectionUtils() {}

  /**
   * Take the logical conjunction of the set of two Integer list.
   */
  public static List<Integer> intersectionFromSortedCollection(
      final List<Integer> list1 , final List<Integer> list2 ) {
    List<Integer> result = new ArrayList<Integer>();
    if ( list1.isEmpty() || list2.isEmpty() ) {
      return result;
    }

    List<Integer> loopTarget;
    List<Integer> compreTarget;
    if ( list1.size() < list2.size() ) {
      loopTarget = list1;
      compreTarget = list2;
    } else {
      loopTarget = list2;
      compreTarget = list1;
    }
    int compreTargetIndex = 0;
    for ( int i = 0 ; i < loopTarget.size() ; i++ ) {
      int loopIndex = loopTarget.get( i ).intValue();
      while ( compreTarget.get( compreTargetIndex ).intValue() < loopIndex ) {
        compreTargetIndex++;
        if ( compreTargetIndex == compreTarget.size() ) {
          break;
        }
      }
      if ( compreTargetIndex == compreTarget.size() ) {
        break;
      }
      if ( loopIndex == compreTarget.get( compreTargetIndex ).intValue() ) {
        result.add( Integer.valueOf( loopIndex ) );
      }
    }

    return result;
  }

  /**
   * Take the logical disjunction of the set of two Integer list.
   */
  public static List<Integer> unionFromSortedCollection(
      final List<Integer> list1 , final List<Integer> list2 ) {
    List<Integer> result = new ArrayList<Integer>();
    List<Integer> loopTarget;
    List<Integer> compreTarget;
    if ( list1.size() < list2.size() ) {
      loopTarget = list2;
      compreTarget = list1;
    } else {
      loopTarget = list1;
      compreTarget = list2;
    }

    int compreTargetIndex = 0;
    for ( int i = 0 ; i < loopTarget.size() ; i++ ) {
      if ( compreTargetIndex == compreTarget.size() ) {
        for ( int ii = i ; ii < loopTarget.size() ; ii++ ) {
          result.add( loopTarget.get( ii ) );
        }
        break;
      }
      int loopIndex = loopTarget.get( i ).intValue();
      while ( compreTarget.get( compreTargetIndex ).intValue() < loopIndex ) {
        result.add( compreTarget.get( compreTargetIndex ) );
        compreTargetIndex++;
        if ( compreTargetIndex == compreTarget.size() ) {
          break;
        }
      }
      result.add( loopTarget.get( i ) );
    }
    if ( compreTargetIndex < compreTarget.size() ) {
      for ( int i = compreTargetIndex ; i < compreTarget.size() ; i++ ) {
        result.add( compreTarget.get( i ) );
      }
    }
    return result;
  }

}
