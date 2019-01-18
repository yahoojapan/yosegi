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

import java.util.HashMap;
import java.util.Map;

public class SpreadLink {

  private final Map<String,ICell> result;
  private final Spread spread;
  private final int index;

  /**
   * Initialization by setting reference of row.
   */
  public SpreadLink( final Spread spread , final int index ) {
    this.spread = spread;
    this.index = index;
    result = new HashMap<String,ICell>();
  }

  public boolean containsColumn( final String columnName ) {
    return spread.containsColumn( columnName );
  }

  /**
   * Get the spread data of this object's row.
   */
  public Map<String,ICell> getLine() {
    if ( result.isEmpty() ) {
      spread.getLine( result , index );
    }
    return result;
  }


}
