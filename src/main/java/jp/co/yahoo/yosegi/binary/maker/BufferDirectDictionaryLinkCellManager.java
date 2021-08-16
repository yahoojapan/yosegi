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
import jp.co.yahoo.yosegi.spread.column.filter.INullFilter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.IntBuffer;

public class BufferDirectDictionaryLinkCellManager implements ICellManager<ICell> {

  private final ColumnType columnType;
  private final ICellMaker cellMaker;
  private final IDicManager dicManager;
  private final IntBuffer dicIndexIntBuffer;
  private final int indexSize;

  /**
   * Object for managing cells from IntBuffer.
   */
  public BufferDirectDictionaryLinkCellManager(
      final ColumnType columnType ,
      final IDicManager dicManager ,
      final IntBuffer dicIndexIntBuffer ) throws IOException {
    this.columnType = columnType;
    this.dicManager = dicManager;
    this.dicIndexIntBuffer = dicIndexIntBuffer;
    cellMaker = CellMakerFactory.getCellMaker( columnType );
    indexSize = dicIndexIntBuffer.capacity();
  }

  @Override
  public void add(final ICell cell , final int index ) {
    throw new UnsupportedOperationException( "read only." );
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ) {
    if ( indexSize <= index ) {
      return defaultCell;
    }
    int dicIndex = dicIndexIntBuffer.get( index );
    if ( dicIndex == 0 ) {
      return defaultCell;
    }
    try {
      return cellMaker.create( dicManager.get( dicIndex ) );
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
  public PrimitiveObject[] getPrimitiveObjectArray(
      final int start ,
      final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];
    int loopEnd = ( start + length );
    for ( int i = start , index = 0 ; i < loopEnd ; i++,index++ ) {
      if ( indexSize <= i ) {
        break;
      }
      int dicIndex = dicIndexIntBuffer.get( i);
      if ( dicIndex != 0 ) {
        try {
          result[index] = dicManager.get( dicIndex );
        } catch ( IOException ex ) {
          throw new UncheckedIOException( ex );
        }
      }
    }
    return result;
  }

  @Override
  public void setPrimitiveObjectArray( 
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ) {
    int loopEnd = ( start + length );
    int index = 0;
    for ( int i = start ; i < loopEnd ; i++,index++ ) {
      if ( indexSize <= i ) {
        break;
      }
      int dicIndex = dicIndexIntBuffer.get( i );
      try {
        if ( dicIndex == 0 ) {
          allocator.setNull( index );
        } else {
          allocator.setPrimitiveObject( index , dicManager.get( dicIndex ) );
        }
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
      }
    }
    for ( int i = index ; i < length ; i++ ) {
      allocator.setNull( i );
    }
  }

}
