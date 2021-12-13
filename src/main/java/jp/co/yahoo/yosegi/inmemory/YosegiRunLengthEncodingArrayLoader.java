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

package jp.co.yahoo.yosegi.inmemory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.ICellManager;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadArrayLink;

import java.io.IOException;

public class YosegiRunLengthEncodingArrayLoader implements IRunLengthEncodingArrayLoader<IColumn> {

  private class ArrayCellManager implements ICellManager<ICell> {

    private final ICell[] cellArray;

    /**
     * Manage it as an Array cell.
     */
    public ArrayCellManager( final ICell[] cellArray ) {
      this.cellArray = cellArray;
    }

    @Override
    public void add( final ICell cell , final int index ) {
      throw new UnsupportedOperationException( "read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ) {
      if ( cellArray.length <= index ) {
        return defaultCell;
      }

      if ( cellArray[index] == null ) {
        return defaultCell;
      }
      return cellArray[index];
    }

    @Override
    public int size() {
      return cellArray.length;
    }

    @Override
    public void clear() {}

  }

  private ICell[] arrayCells;
  private final ArrayColumn arrayColumn;
  private final int loadSize;
  protected final Spread spread;

  /**
   * A loader that holds elements dictionary.
   */
  public YosegiRunLengthEncodingArrayLoader(
      final ColumnBinary columnBinary , final int loadSize ) {
    arrayColumn = new ArrayColumn( columnBinary.columnName );
    spread = new Spread( arrayColumn );
    arrayCells = new ICell[loadSize];
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void finish() throws IOException {
    spread.setRowCount( loadSize );
    arrayColumn.setSpread( spread );
    arrayColumn.setCellManager( new ArrayCellManager( arrayCells ) );
  }

  @Override
  public IColumn build() throws IOException {
    return arrayColumn;
  }

  @Override
  public void setRowGroupCount( final int count ) throws IOException {}

  @Override
  public void setNullAndRepetitions(
      final int startIndex ,
      final int repetitions ,
      final int rowGroupIndex )  throws IOException {
    for ( int i = startIndex ; i < startIndex + repetitions ; i++ ) {
      arrayCells[i] = null;
    }
  }

  @Override
  public void setRowGourpIndexAndRepetitions(
      final int startIndex ,
      final int repetitions ,
      final int rowGroupIndex ,
      final int rowGroupStart ,
      final int rowGourpLength ) throws IOException {
    for ( int i = startIndex ; i < startIndex + repetitions ; i++ ) {
      arrayCells[i] = new ArrayCell(
          new SpreadArrayLink(
              spread ,
              rowGroupIndex ,
              rowGroupStart ,
              rowGroupStart + rowGourpLength ) );
    }
  }

  @Override
  public void loadChild(
      final ColumnBinary columnBinary , final int childLoadSize ) throws IOException {
    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn child = factory.create( columnBinary , childLoadSize );
    child.setParentsColumn( arrayColumn );
    spread.addColumn( child );
  }

}
