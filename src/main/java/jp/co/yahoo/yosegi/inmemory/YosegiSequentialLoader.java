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

public class YosegiSequentialLoader implements ISequentialLoader<IColumn> {

  private class SequentialCellManager implements ICellManager<ICell> {

    private final PrimitiveObject[] valueArray;
    private final ICellMaker cellMaker;

    private SequentialCellManager(
        final ColumnType columnType , final PrimitiveObject[] valueArray ) throws IOException {
      this.valueArray = valueArray;
      cellMaker = CellMakerFactory.getCellMaker( columnType );
    }

    @Override
    public void add( final ICell cell , final int index ) {
      throw new UnsupportedOperationException( "read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ) {
      if ( valueArray.length <= index
          || valueArray[index] == null ) {
        return defaultCell;
      }
      return cellMaker.create( valueArray[index] );
    }

    @Override
    public int size() {
      return valueArray.length;
    }

    @Override
    public void clear() {}

  }

  private final ColumnType columnType;
  private final String columnName;
  private final int loadSize;
  private final PrimitiveObject[] values;

  /**
   * A loader that holds elements sequentially.
   */
  public YosegiSequentialLoader( final ColumnBinary columnBinary , final int loadSize ) {
    this.columnName = columnBinary.columnName;
    this.columnType = columnBinary.columnType;
    this.loadSize = loadSize;
    values = new PrimitiveObject[loadSize];
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public IColumn build() throws IOException {
    IColumn column = new PrimitiveColumn( columnType , columnName );
    column.setCellManager( new SequentialCellManager( columnType , values ) );
    return column;
  }

  @Override
  public void finish() throws IOException {
  }

  @Override
  public void setNull( final int index ) throws IOException {
    values[index] = null;
  }

  @Override
  public void setBoolean( final int index , final boolean value ) throws IOException {
    values[index] = new BooleanObj( value );
  }

  @Override
  public void setByte( final int index , final byte value ) throws IOException {
    values[index] = new ByteObj( value );
  }

  @Override
  public void setShort( final int index , final short value ) throws IOException {
    values[index] = new ShortObj( value );
  }

  @Override
  public void setInteger( final int index , final int value ) throws IOException {
    values[index] = new IntegerObj( value );
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException {
    values[index] = new LongObj( value );
  }

  @Override
  public void setFloat( final int index , final float value ) throws IOException {
    values[index] = new FloatObj( value );
  }

  @Override
  public void setDouble( final int index , final double value ) throws IOException {
    values[index] = new DoubleObj( value );
  }


  @Override
  public void setBytes(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    values[index] = new Utf8BytesLinkObj( value , start , length );
  }

  @Override
  public void setString( final int index , final String value ) throws IOException {
    values[index] = new StringObj( value );
  }

}
