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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.ISettableIndexParser;
import jp.co.yahoo.yosegi.message.parser.PrimitiveConverter;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;

public class YosegiArrayParser implements ISettableIndexParser {

  private final IColumn column;
  private final IColumn arrayColumn;

  private int start;
  private int length;

  public YosegiArrayParser( final IColumn column ) {
    this.column = column;
    arrayColumn = column.getColumn(0);
  }

  @Override
  public void setIndex( final int index ) {
    ICell cell = column.get( index );
    if ( cell.getType() == ColumnType.ARRAY ) {
      ArrayCell arrayCell = (ArrayCell)cell;
      start = arrayCell.getStart();
      length = arrayCell.getEnd() - start;
    } else {
      start = 0;
      length = 0;
    }
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException {
    return null;
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return (index < length ) ? PrimitiveConverter.convert(arrayColumn.get(start + index)) : null;
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    return YosegiNullParser.getInstance();
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    if ( length <= index ) {
      return YosegiNullParser.getInstance();
    }
    int target = start + index;
    ISettableIndexParser parser = YosegiParserFactory.get( arrayColumn , target );
    parser.setIndex( target );

    return parser;
  }

  @Override
  public String[] getAllKey() throws IOException {
    return new String[0];
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return false;
  }

  @Override
  public int size() throws IOException {
    return length;
  }

  @Override
  public boolean isArray() throws IOException {
    return true;
  }

  @Override
  public boolean isMap() throws IOException {
    return false;
  }

  @Override
  public boolean isStruct() throws IOException {
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    if ( length <= index ) {
      return false;
    }
    int target = start + index;
    return YosegiParserFactory.hasParser( arrayColumn , target );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    return false;
  }

  @Override
  public Object toJavaObject() throws IOException {
    return null;
  }

}
