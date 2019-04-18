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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.message.parser.ISettableIndexParser;
import jp.co.yahoo.yosegi.spread.column.IColumn;

public final class YosegiParserFactory {

  private YosegiParserFactory() {}

  /**
   * Convert a column to a parser object.
   */
  public static ISettableIndexParser get(final IColumn column , final int index ) {
    if ( column == null ) {
      return null;
    }
    switch ( column.getColumnType() ) {
      case SPREAD:
        return new YosegiSpreadParser( column );
      case ARRAY:
        return new YosegiArrayParser( column );
      case UNION:
        return get( column.getColumn( column.get( index ).getType() ) , index );
      default:
        return YosegiNullParser.getInstance();
    }
  }

  /**
   * Determine whether the target column has a child column.
   */
  public static boolean hasParser( final IColumn column , final int index ) {
    if ( column == null ) {
      return false;
    }
    switch ( column.getColumnType() ) {
      case SPREAD:
        return true;
      case ARRAY:
        return true;
      case UNION:
        return hasParser( column.getColumn( column.get( index ).getType() ) , index );
      default:
        return false;
    }

  }

}
