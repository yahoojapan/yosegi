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

package jp.co.yahoo.yosegi.spread.analyzer;

import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class Analyzer {

  private Analyzer() {}

  /**
   * Obtain an object for analyzing from Spread.
   */
  public static List<IColumnAnalizer> getAnalizer( final Spread spread ) throws IOException {
    List<IColumnAnalizer> result = new ArrayList<IColumnAnalizer>();
    for ( int i = 0 ; i < spread.getColumnSize() ; i++ ) {
      IColumn column = spread.getColumn( i );
      result.add( ColumnAnalizerFactory.get( column ) );
    }
    return result;
  }

  /**
   * Analyze Spread and obtain the result.
   */
  public static List<IColumnAnalizeResult> analize( final Spread spread ) throws IOException {
    List<IColumnAnalizeResult> result = new ArrayList<IColumnAnalizeResult>();
    for ( IColumnAnalizer analizer : getAnalizer( spread ) ) {
      if ( analizer != null ) {
        result.add( analizer.analize() );
      }
    }
    return result;
  }

}
