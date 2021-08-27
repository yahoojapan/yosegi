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

public class YosegiDictionaryLoader implements IDictionaryLoader<IColumn> {

  private class DictionaryCellManager implements ICellManager<ICell> {

    private final PrimitiveObject[] dicArray;
    private final int[] idArray;
    private final boolean[] isNullArray;
    private final ICellMaker cellMaker;

    private DictionaryCellManager(
        final ColumnType columnType ,
        final PrimitiveObject[] dicArray ,
        final int[] idArray ,
        final boolean[] isNullArray ) throws IOException {
      this. dicArray = dicArray;
      this.isNullArray = isNullArray;
      this.idArray = idArray;
      cellMaker = CellMakerFactory.getCellMaker( columnType );
    }

    @Override
    public void add( final ICell cell , final int index ) {
      throw new UnsupportedOperationException( "read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ) {
      if ( isNullArray.length <= index
          || isNullArray[index] ) {
        return defaultCell;
      }
      return cellMaker.create( dicArray[idArray[index]] );
    }

    @Override
    public int size() {
      return isNullArray.length;
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
      //Note: depricate this method.
    }

  }

  private final ColumnType columnType;
  private final String columnName;
  private final int loadSize;
  private final int[] ids;
  private final boolean[] isNull;
  private PrimitiveObject[] dic;

  /**
   * A loader that holds elements dictionary.
   */
  public YosegiDictionaryLoader( final ColumnBinary columnBinary , final int loadSize ) {
    this.columnName = columnBinary.columnName;
    this.columnType = columnBinary.columnType;
    this.loadSize = loadSize;
    isNull = new boolean[loadSize];
    ids = new int[loadSize];
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public IColumn build() throws IOException {
    IColumn column = new PrimitiveColumn( columnType , columnName );
    column.setCellManager( new DictionaryCellManager( columnType , dic , ids , isNull ) );
    return column;
  }

  @Override
  public void finish() throws IOException {
  }

  @Override
  public void setNull( final int index ) throws IOException {
    isNull[index] = true;
  }

  @Override
  public void createDictionary( final int dictionarySize ) throws IOException {
    dic = new PrimitiveObject[dictionarySize];
  }

  @Override
  public void setDictionaryIndex( final int index , final int dicIndex ) throws IOException {
    ids[index] = dicIndex;
  }

  @Override
  public void setNullToDic( final int index ) throws IOException {
    isNull[index] = true;
  }

  @Override
  public void setBooleanToDic( final int index , final boolean value ) throws IOException {
    dic[index] = new BooleanObj( value );
  }

  @Override
  public void setByteToDic( final int index , final byte value ) throws IOException {
    dic[index] = new ByteObj( value );
  }

  @Override
  public void setShortToDic( final int index , final short value ) throws IOException {
    dic[index] = new ShortObj( value );
  }

  @Override
  public void setIntegerToDic( final int index , final int value ) throws IOException {
    dic[index] = new IntegerObj( value );
  }

  @Override
  public void setLongToDic( final int index , final long value ) throws IOException {
    dic[index] = new LongObj( value );
  }

  @Override
  public void setFloatToDic( final int index , final float value ) throws IOException {
    dic[index] = new FloatObj( value );
  }

  @Override
  public void setDoubleToDic( final int index , final double value ) throws IOException {
    dic[index] = new DoubleObj( value );
  }

  @Override
  public void setBytesToDic(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    dic[index] = new Utf8BytesLinkObj( value , start , length );
  }

  @Override
  public void setStringToDic( final int index , final String value ) throws IOException {
    dic[index] = new StringObj( value );
  }

}
