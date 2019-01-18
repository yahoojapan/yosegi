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

package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.spread.Spread;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class SpreadArrayLink {

  private final List<ICell> result;
  private final Spread spread;
  private final IColumn arrayColumn;
  private final int parentIndex;
  private final int start;
  private final int end;

  /**
   * Set information representing a line and initialize it.
   */
  public SpreadArrayLink(
      final Spread spread , final int parentIndex , final int start , final int end ) {
    this.spread = spread;
    arrayColumn = spread.getColumn(0);
    this.parentIndex = parentIndex;
    this.start = start;
    this.end = end;

    result = new ArrayList<ICell>();
  }

  public int getParentIndex() {
    return parentIndex;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public Spread getSpread() {
    return spread;
  }

  public ICell getArrayRow( final int index ) {
    int target = start + index;
    return arrayColumn.get( target );
  }

  /**
   * Get the array data of this object's row.
   */
  public List<ICell> getLine() {
    result.clear();
    if ( result.isEmpty() ) {
      IntStream.range( start , end )
          .forEach( i -> result.add( arrayColumn.get( i ) ) );
    }
    return result;
  }

}
