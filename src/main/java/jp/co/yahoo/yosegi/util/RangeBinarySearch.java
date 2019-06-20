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

public class RangeBinarySearch<T> {

  private final List<IndexAndObject<T>> indexAndObjectList =
      new ArrayList<IndexAndObject<T>>();
  private IndexAndObject<T> currentIndexAndObject;

  private int maxIndex = -1;
  private int firstIndex = -1;

  /**
   * Add an object to the specified index.
   */
  public void add( final T obj , final int index ) {
    if ( index <= maxIndex ) {
      throw new RuntimeException( "This class only allows additions." );
    }

    if ( firstIndex == -1 ) {
      firstIndex = index;
    }
    if ( currentIndexAndObject == null || currentIndexAndObject.getNextIndex() != index ) {
      currentIndexAndObject = new IndexAndObject<T>( index );
      indexAndObjectList.add( currentIndexAndObject );
    }
    currentIndexAndObject.add( obj );
    maxIndex = index;
  }

  /**
   * Get the object of the specified index.
   */
  public T get( final int index ) {
    if ( firstIndex <= index && index <= maxIndex ) {
      return search( index , 0 , indexAndObjectList.size() - 1 );
    } else {
      return null;
    }
  }

  private T search( final int index , final int min , final int max ) {
    if ( max < min ) {
      return null;
    } else {
      int mid = min + ( max - min ) / 2;
      int hasIndex = indexAndObjectList.get( mid ).hasIndex( index );
      if ( 0 < hasIndex ) {
        return search( index , mid + 1 , max );
      } else if ( hasIndex < 0 ) {
        return search( index , min , mid - 1 );
      } else {
        return indexAndObjectList.get( mid ).get( index );
      }
    }
  }

  /**
   * Get the size of the list.
   */
  public int size() {
    if ( indexAndObjectList.isEmpty() ) {
      return 0;
    }
    return maxIndex + 1;
  }

  /**
   * Clears the variable.
   */
  public void clear() {
    indexAndObjectList.clear();
    currentIndexAndObject = null;
    maxIndex = 0;
    firstIndex = -1;
  }

  public List<IndexAndObject<T>> getIndexAndObjectList() {
    return indexAndObjectList;
  }

}
