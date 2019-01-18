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

import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;

import java.io.IOException;

public class BooleanColumnAnalizer implements IColumnAnalizer {

  private final IColumn column;

  public BooleanColumnAnalizer( final IColumn column ) {
    this.column = column;
  }

  @Override
  public IColumnAnalizeResult analize() throws IOException {
    boolean maybeSorted = true;
    Boolean currentSortCheckValue = false;
    int trueCount = 0;
    int falseCount = 0;
    int nullCount = 0;
    int rowCount = 0;

    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        continue;
      }
      boolean target = ( (PrimitiveCell)cell ).getRow().getBoolean();
      if ( maybeSorted && currentSortCheckValue.compareTo( target ) <= 0 ) {
        currentSortCheckValue = target;
      } else {
        maybeSorted = false;
      }

      if ( target ) {
        trueCount++;
        rowCount++;
      } else {
        falseCount++;
        rowCount++;
      }
    }

    int uniqCount = 0;
    if ( trueCount != 0 ) {
      uniqCount++;
    }
    if ( falseCount != 0 ) {
      uniqCount++;
    }

    return new BooleanColumnAnalizeResult(
        column.getColumnName() ,
        column.size() ,
        maybeSorted ,
        nullCount ,
        rowCount ,
        uniqCount ,
        trueCount ,
        falseCount );
  }

}
