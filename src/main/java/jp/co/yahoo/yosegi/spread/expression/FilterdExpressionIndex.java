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

package jp.co.yahoo.yosegi.spread.expression;

public class FilterdExpressionIndex implements IExpressionIndex {

  private final int[] indexList;
  private int size;

  /**
   * Create a valid row index from row flags.
   */
  public FilterdExpressionIndex( final boolean[] filterArray ) {
    size = 0;
    indexList = new int[filterArray.length];
    for ( int i = 0 ; i < filterArray.length ; i++ ) {
      if ( filterArray[i] ) {
        indexList[size] = i;
        size++;
      }
    }
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int get( final int index ) {
    return indexList[index];
  }

}
