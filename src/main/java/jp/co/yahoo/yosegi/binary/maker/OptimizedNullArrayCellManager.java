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

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.CellMakerFactory;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.ICellMaker;
import jp.co.yahoo.yosegi.spread.column.ICellManager;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.DefaultCellIndex;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import java.io.IOException;
import java.io.UncheckedIOException;

public class OptimizedNullArrayCellManager implements ICellManager<ICell> {

  private final ColumnType columnType;
  private final ICellMaker cellMaker;
  private final int startIndex;
  private final PrimitiveObject[] valueArray;

  private ICellIndex index = new DefaultCellIndex();

  /**
   * ColumnMaker's cell manager compatible with OptimizedNullArray.
   */
  public OptimizedNullArrayCellManager(
      final ColumnType columnType ,
      final int startIndex,
      final PrimitiveObject[] valueArray ) throws IOException {
    this.columnType = columnType;
    this.startIndex = startIndex;
    this.valueArray = valueArray;
    cellMaker = CellMakerFactory.getCellMaker( columnType );
  }

  @Override
  public void add( final ICell cell , final int index ) {
    throw new UnsupportedOperationException( "read only." );
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ) {
    if ( index < startIndex
        || ( valueArray.length + startIndex ) <= index
        || valueArray[index - startIndex] == null ) {
      return defaultCell;
    }
    return cellMaker.create( valueArray[index - startIndex] );
  }

  @Override
  public int size() {
    return valueArray.length + startIndex;
  }

  @Override
  public void clear() {}

  @Override
  public void setIndex( final ICellIndex index ) {
    this.index = index;
  }

  @Override
  public boolean[] filter(
      final IFilter filter , final boolean[] filterArray ) throws IOException {
    return index.filter( filter , filterArray );
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];
    int loopEnd = ( start + length );
    if ( size() < loopEnd ) {
      loopEnd = size();
    }
    for ( int i = start,index = 0 ; i < loopEnd ; i++,index++ ) {
      int valueIndex = indexList.get( i );
      if ( valueIndex < startIndex 
          || size() <= valueIndex
          || valueArray[ valueIndex - startIndex ] == null ) {
        continue;
      }
      result[index] = valueArray[valueIndex - startIndex];
    }
    return result;
  }

  @Override
  public void setPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ,
      final IMemoryAllocator allocator ) {
    int loopEnd = ( start + length );
    if ( size() < loopEnd ) {
      loopEnd = size();
    }
    int index = 0;
    for ( int i = start ; i < loopEnd ; i++,index++ ) {
      if ( i < startIndex || valueArray[ i - startIndex ] == null ) {
        allocator.setNull( index );
      } else {
        try {
          allocator.setPrimitiveObject( index , valueArray[i - startIndex] );
        } catch ( IOException ex ) {
          throw new UncheckedIOException( ex );
        }
      }
    }
    for ( int i = index ; i < length ; i++ ) {
      allocator.setNull( i );
    }
  }

}
