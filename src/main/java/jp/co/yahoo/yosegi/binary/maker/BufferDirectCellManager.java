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
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.DefaultCellIndex;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import java.io.IOException;
import java.io.UncheckedIOException;

public class BufferDirectCellManager implements ICellManager<ICell> {

  private final ColumnType columnType;
  private final ICellMaker cellMaker;
  private final IDicManager dicManager;
  private final int indexSize;

  private ICellIndex index = new DefaultCellIndex();

  /**
   * Object for managing cells from IntBuffer.
   */
  public BufferDirectCellManager(
      final ColumnType columnType ,
      final IDicManager dicManager ,
        final int indexSize ) throws IOException {
    this.columnType = columnType;
    this.dicManager = dicManager;
    this.indexSize = indexSize;
    cellMaker = CellMakerFactory.getCellMaker( columnType );
  }

  @Override
  public void add( final ICell cell , final int index ) {
    throw new UnsupportedOperationException( "read only." );
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ) {
    if ( indexSize <= index ) {
      return defaultCell;
    }
    try {
      PrimitiveObject obj = dicManager.get( index );
      if ( obj == null ) {
        return defaultCell;
      }
      return cellMaker.create( dicManager.get( index ) );
    } catch ( IOException ex ) {
      throw new RuntimeException( ex );
    }
  }

  @Override
  public int size() {
    return indexSize;
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
    switch ( filter.getFilterType() ) {
      case NOT_NULL:
        return null;
      case NULL:
        return null;
      default:
        return index.filter( filter , filterArray );
    }
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];
    int loopEnd = ( start + length );
    if ( indexList.size() < loopEnd ) {
      loopEnd = indexList.size();
    }
    for ( int i = start , index = 0 ; i < loopEnd ; i++,index++ ) {
      int targetIndex = indexList.get( i );
      if ( indexSize <= targetIndex ) {
        break;
      }
      try {
        result[index] = dicManager.get( targetIndex );
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
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
    int loopEnd = ( start + length );
    if ( indexList.size() < loopEnd ) {
      loopEnd = indexList.size();
    }
    try {
      int index = 0;
      for ( int i = start ; i < loopEnd ; i++,index++ ) {
        int targetIndex = indexList.get( i );
        if ( indexSize <= targetIndex ) {
          break;
        }
        PrimitiveObject obj = dicManager.get( targetIndex );
        if ( obj == null ) {
          allocator.setNull( index );
        } else {
          allocator.setPrimitiveObject( index , obj );
        }
      }
      for ( int i = index ; i < length ; i++ ) {
        allocator.setNull( i );
      }
    } catch ( IOException ex ) {
      throw new UncheckedIOException( ex );
    }
  }

}
