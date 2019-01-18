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
import java.util.HashSet;
import java.util.Set;

public class ByteColumnAnalizer implements IColumnAnalizer {

  private final IColumn column;

  public ByteColumnAnalizer( final IColumn column ) {
    this.column = column;
  }

  @Override
  public IColumnAnalizeResult analize() throws IOException {
    boolean maybeSorted = true;
    Byte currentSortCheckValue = Byte.MIN_VALUE;
    int nullCount = 0;
    int rowCount = 0;

    Set<Byte> dicSet = new HashSet<Byte>();

    Byte min = Byte.MAX_VALUE;
    Byte max = Byte.MIN_VALUE;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        continue;
      }
      Byte target = Byte.valueOf( ( (PrimitiveCell) cell).getRow().getByte() );
      if ( maybeSorted && currentSortCheckValue.compareTo( target ) <= 0 ) {
        currentSortCheckValue = target;
      } else {
        maybeSorted = false;
      }

      rowCount++;
      if ( ! dicSet.contains( target ) ) {
        dicSet.add( target );
        if ( 0 < min.compareTo( target ) ) {
          min = Byte.valueOf( target );
        }
        if ( max.compareTo( target ) < 0 ) {
          max = Byte.valueOf( target );
        }
      }
    }

    int uniqCount = dicSet.size();

    return new ByteColumnAnalizeResult(
        column.getColumnName() ,
        column.size() ,
        maybeSorted ,
        nullCount ,
        rowCount ,
        uniqCount ,
        min ,
        max );
  }

}
