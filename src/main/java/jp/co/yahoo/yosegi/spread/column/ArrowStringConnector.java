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

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.BytesStringObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import org.apache.arrow.vector.VarCharVector;

import java.io.IOException;

public class ArrowStringConnector implements IArrowPrimitiveConnector {

  private final String columnName;
  private final VarCharVector vector;

  public ArrowStringConnector( final String columnName , final VarCharVector vector ) {
    this.columnName = columnName;
    this.vector = vector;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public ColumnType getColumnType() {
    return ColumnType.STRING;
  }

  @Override
  public void add( final ICell cell , final int index ) {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ) {
    if ( vector.isNull( index ) ) {
      return defaultCell;
    }
    return new PrimitiveCell( ColumnType.STRING , new BytesStringObj( vector.get( index ) ) );
  }

  @Override
  public int size() {
    return vector.getValueCount();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public void setIndex( final ICellIndex index ) {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList , final int start , final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];
    for ( int i = start ; i < ( start + length ) && i < size() ; i++ ) {
      if ( ! vector.isNull( i ) ) {
        result[i - start] =  new BytesStringObj( vector.get( i ) );
      }
    }
    return result;
  }

  @Override
  public void setPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ,
      final IMemoryAllocator allocator ) {
    for ( int i = start ; i < ( start + length ) && i < size() ; i++ ) {
      if ( vector.isNull( i ) ) {
        allocator.setNull( i - start );
      } else {
        try {
          allocator.setPrimitiveObject( i - start , new BytesStringObj( vector.get( i ) ) );
        } catch ( IOException ex ) {
          throw new RuntimeException( ex );
        }
      }
    }
  }

}
