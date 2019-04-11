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
import jp.co.yahoo.yosegi.spread.column.IColumn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class YosegiSpreadParser implements ISettableIndexParser {

  private final IColumn column;
  private final Map<String,ISettableIndexParser> cache;

  private int currentIndex;

  public YosegiSpreadParser( final IColumn column ) {
    this.column = column;
    cache = new HashMap<String,ISettableIndexParser>();
  }

  @Override
  public void setIndex( final int index ) {
    currentIndex = index;
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException {
    return PrimitiveConverter.convert( column.getColumn( key ).get( currentIndex ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return null;
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    ISettableIndexParser parser = cache.get( key );
    if ( Objects.isNull(parser) ) {
      parser = YosegiParserFactory.get( column.getColumn( key ) , currentIndex );
      cache.put( key , parser );
    }
    parser.setIndex( currentIndex );
 
    return parser;
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    return YosegiNullParser.getInstance();
  }

  @Override
  public String[] getAllKey() throws IOException {
    return column.getColumnKeys().toArray( new String[ column.getColumnSize() ] );
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return Objects.nonNull(get(key));
  }

  @Override
  public int size() throws IOException {
    return column.getColumnSize();
  }

  @Override
  public boolean isArray() throws IOException {
    return false;
  }

  @Override
  public boolean isMap() throws IOException {
    return true;
  }

  @Override
  public boolean isStruct() throws IOException {
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    return false;
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    return YosegiParserFactory.hasParser( column.getColumn( key ) , currentIndex );
  }

  @Override
  public Object toJavaObject() throws IOException {
    return null;
  }

}
