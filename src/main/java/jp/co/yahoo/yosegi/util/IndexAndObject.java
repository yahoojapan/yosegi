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

public class IndexAndObject<T> {

  private final int startIndex;
  private final List<T> list;
  private int maxIndex = 0;

  /**
   * Set the index of start and initialize.
   */
  public IndexAndObject( final int startIndex ) {
    if ( startIndex < 0 ) {
      throw new RuntimeException( "The start index must be greater than or equal to 0." );
    }
    this.startIndex = startIndex;
    list = new ArrayList<T>();
    maxIndex += startIndex;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int size() {
    return list.size();
  }

  public void add( final T obj ) {
    maxIndex++;
    list.add( obj );
  }

  public T get( final int index ) {
    return list.get( index - startIndex );
  }

  /**
   * Check if it has an argument index.
   */
  public int hasIndex( final int target ) {
    if ( target < startIndex ) {
      return -1;
    } else if ( maxIndex <= target ) {
      return 1;
    } else {
      return 0;
    }
  }

  public int getNextIndex() {
    return startIndex + list.size();
  }

}
