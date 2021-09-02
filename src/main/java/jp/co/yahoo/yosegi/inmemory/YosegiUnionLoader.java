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
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.UnionColumn;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

public class YosegiUnionLoader implements IUnionLoader<IColumn> {

  private final UnionColumn unionColumn;
  private final Map<ColumnType,IColumn> columnContainer;
  private final int loadSize;

  /**
   * A loader that holds elements dictionary.
   */
  public YosegiUnionLoader( final ColumnBinary columnBinary , final int loadSize ) {
    columnContainer = new EnumMap<>( ColumnType.class );
    unionColumn = new UnionColumn( columnBinary.columnName , columnContainer );
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull( final int index ) throws IOException {
    unionColumn.addCell( ColumnType.NULL , null , index );
  }

  @Override
  public void finish() throws IOException {}

  @Override
  public IColumn build() throws IOException {
    return unionColumn;
  }

  @Override
  public void setIndexAndColumnType(
      final int index , final ColumnType columnType ) throws IOException {
    if ( columnContainer.containsKey( columnType ) ) {
      unionColumn.addCell( columnType , columnContainer.get( columnType ).get( index ) , index );
    }
  }

  @Override
  public void loadChild(
      final ColumnBinary columnBinary , final int childLoadSize ) throws IOException {
    YosegiLoaderFactory factory = new YosegiLoaderFactory();
    IColumn child = factory.create( columnBinary , childLoadSize );
    child.setParentsColumn( unionColumn );
    unionColumn.setColumn( child );
    columnContainer.put( child.getColumnType() , child );
  }

}
