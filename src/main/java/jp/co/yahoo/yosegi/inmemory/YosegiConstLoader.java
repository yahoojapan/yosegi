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
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.Utf8BytesLinkObj;
import jp.co.yahoo.yosegi.spread.column.CellMakerFactory;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.ICellMaker;
import jp.co.yahoo.yosegi.spread.column.ICellManager;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;

import java.io.IOException;

public class YosegiConstLoader implements IConstLoader<IColumn> {

  private class ConstCellManager implements ICellManager<ICell> {

    private final ICell cell;
    private final PrimitiveObject value;
    private final int length;

    /**
     * Create a const value cell manager.
     */
    public ConstCellManager(
        final ColumnType columnType ,
        final PrimitiveObject value ,
        final int length ) throws IOException {
      this.value = value;
      this.length = length;
      ICellMaker cellMaker = CellMakerFactory.getCellMaker( columnType );
      cell = cellMaker.create( value );
    }

    @Override
    public void add( final ICell cell , final int index ) {
      throw new UnsupportedOperationException( "Constant column is read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ) {
      if ( length <= index || value == null ) {
        return defaultCell;
      }

      return cell;
    }

    @Override
    public int size() {
      return length;
    }

    @Override
    public void clear() {}

    @Override
    public PrimitiveObject[] getPrimitiveObjectArray(
        final int start ,
        final int length ) {
      // Note: depricate this method.
      return new PrimitiveObject[length];
    }

    @Override
    public void setPrimitiveObjectArray(
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ) {
      // Note: depricate this method.
    }

  }

  private final ColumnType columnType;
  private final String columnName;
  private final int loadSize;
  private PrimitiveObject constValue;

  /**
   * A loader that holds elements sequentially.
   */
  public YosegiConstLoader( final ColumnBinary columnBinary , final int loadSize ) {
    this.columnName = columnBinary.columnName;
    this.columnType = columnBinary.columnType;
    this.loadSize = loadSize;
    constValue = null;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public IColumn build() throws IOException {
    ICellManager cellManager =
        new ConstCellManager( columnType , constValue , loadSize );
    IColumn column =
        new PrimitiveColumn( columnType , columnName );
    column.setCellManager( cellManager );
    return column;
  }

  @Override
  public void setNull( final int index ) throws IOException {}

  @Override
  public void finish() throws IOException {}

  @Override
  public void setConstFromNull() throws IOException {}

  @Override
  public void setConstFromBoolean( final boolean value ) throws IOException {
    constValue = new BooleanObj( value );
  }

  @Override
  public void setConstFromByte( final byte value ) throws IOException {
    constValue = new ByteObj( value );
  }

  @Override
  public void setConstFromShort( final short value ) throws IOException {
    constValue = new ShortObj( value );
  }

  @Override
  public void setConstFromInteger( final int value ) throws IOException {
    constValue = new IntegerObj( value );
  }

  @Override
  public void setConstFromLong( final long value ) throws IOException {
    constValue = new LongObj( value );
  }

  @Override
  public void setConstFromFloat( final float value ) throws IOException {
    constValue = new FloatObj( value );
  }

  @Override
  public void setConstFromDouble( final double value ) throws IOException {
    constValue = new DoubleObj( value );
  }

  @Override
  public void setConstFromBytes(
      final byte[] value , final int start , final int length ) throws IOException {
    constValue = new Utf8BytesLinkObj( value , start , length );
  }

  @Override
  public void setConstFromString( final String value ) throws IOException {
    constValue = new StringObj( value );
  }

}
