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
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.DefaultCellIndex;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CellManager implements ICellManager {

  private final List<CellIndex> cellIndex = new ArrayList<CellIndex>();
  private ICellIndex index = new DefaultCellIndex();
  private CellIndex currentCellIndex;
  private int maxIndex;
  private int firstIndex = -1;

  @Override
  public void add( final ICell cell , final int index ) {
    if ( firstIndex == -1 ) {
      firstIndex = index;
    }
    if ( currentCellIndex == null || currentCellIndex.getNextIndex() != index ) {
      currentCellIndex = new CellIndex( index );
      cellIndex.add( currentCellIndex );
    }
    currentCellIndex.add( cell );
    if ( maxIndex < index ) {
      maxIndex = index;
    }
  }

  @Override
  public ICell get( final int index , final ICell defaultCell ) {
    if ( firstIndex <= index && index <= maxIndex ) {
      return search( index , 0 , cellIndex.size() - 1 , defaultCell );
    } else {
      return defaultCell;
    }
  }

  @Override
  public int size() {
    if ( cellIndex.isEmpty() ) {
      return 0;
    }
    return maxIndex + 1;
  }

  @Override
  public void clear() {
    cellIndex.clear();
    currentCellIndex = null;
    maxIndex = 0;
    firstIndex = -1;
  }

  @Override
  public void setIndex( final ICellIndex index ) {
    this.index = index;
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException {
    switch ( filter.getFilterType() ) {
      case NOT_NULL:
        for ( CellIndex index : cellIndex ) {
          index.setFilter( filterArray );
        }
        return filterArray;
      case NULL:
        return null;
      default:
        return index.filter( filter , filterArray );
    }
  }

  private ICell search(
      final int index , final int min , final int max , final ICell defaultCell ) {
    if ( max < min ) {
      return defaultCell;
    } else {
      int mid = min + ( max - min ) / 2;
      int hasIndex = cellIndex.get( mid ).hasIndex( index );
      if ( 0 < hasIndex ) {
        return search( index , mid + 1 , max , defaultCell );
      } else if ( hasIndex < 0 ) {
        return search( index , min , mid - 1 , defaultCell );
      } else {
        return cellIndex.get( mid ).get( index );
      }
    }
  }

  private class CellIndex {
    private final int startIndex;
    private final List<ICell> cellList;
    private int maxIndex = 0;

    public int getStartIndex() {
      return startIndex;
    }

    public CellIndex( final int startIndex ) {
      this.startIndex = startIndex;
      cellList = new ArrayList<ICell>();
      maxIndex += startIndex;
    }

    public void add( final ICell cell ) {
      maxIndex++;
      cellList.add( cell );
    }

    public ICell get( final int index ) {
      return cellList.get( index - startIndex );
    }

    public int hasIndex( final int target ) {
      if ( target < startIndex ) {
        return -1;
      } else if ( maxIndex <= target ) {
        return 1;
      } else {
        return 0;
      }
    }

    public void setFilter( final boolean[] filterArray ) {
      for ( int i = 0 ; i < cellList.size() ; i++ ) {
        filterArray[ i + startIndex ] = true;
      }
    }

    public int getNextIndex() {
      return startIndex + cellList.size();
    }
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList , final int start , final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];
    for ( int i = 0,index = start ; i < length ; i++,index++ ) {
      Object obj = get( index , NullCell.getInstance() ).getRow();
      if ( obj instanceof PrimitiveObject ) {
        result[i] = (PrimitiveObject)obj;
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
    for ( int i = 0,index = start ; i < length ; i++,index++ ) {
      Object obj = get( index , NullCell.getInstance() ).getRow();
      try {
        if ( obj instanceof PrimitiveObject ) {
          allocator.setPrimitiveObject( i , (PrimitiveObject)obj );
        } else {
          allocator.setNull( i );
        }
      } catch ( IOException ex ) {
        throw new RuntimeException( ex );
      }
    }
  }

}
