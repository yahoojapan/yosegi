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

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public final class NullColumn implements IColumn {

  private static final NullColumn NULL_COLUMN = new NullColumn( 0 );

  private final int rowCount;
  private IColumn parentsColumn = NullColumn.getInstance();
  private ICell defaultCell = NullCell.getInstance();

  private NullColumn( final int rowCount ) {
    this.rowCount = rowCount;
  }

  public static IColumn getInstance() {
    return NULL_COLUMN;
  }

  public static IColumn getInstance( final int rowCount ) {
    return new NullColumn( rowCount );
  }

  @Override
  public void setColumnName( final String columnName ) {
  }

  @Override
  public String getColumnName() {
    return ColumnType.NULL.toString();
  }

  @Override
  public ColumnType getColumnType() {
    return ColumnType.NULL;
  }

  @Override
  public void setParentsColumn( final IColumn parentsColumn ) {
    this.parentsColumn = parentsColumn;
  }

  @Override
  public IColumn getParentsColumn() {
    return parentsColumn;
  }

  @Override
  public int add( final ColumnType type , final Object obj , final int index ) throws IOException {
    return 0;
  }

  @Override
  public void addCell(
      final ColumnType type , final ICell cell , final int index ) throws IOException {}

  @Override
  public ICellManager getCellManager() {
    return null;
  }

  @Override
  public void setCellManager( final ICellManager cellManager ) {
  }

  @Override
  public ICell get( final int index ) {
    return defaultCell;
  }

  @Override
  public List<String> getColumnKeys() {
    return new ArrayList<String>();
  }

  @Override
  public int getColumnSize() {
    return 0;
  }

  @Override
  public List<IColumn> getListColumn() {
    return new ArrayList<IColumn>();
  }

  @Override
  public IColumn getColumn( final int index ) {
    return getInstance();
  }

  @Override
  public IColumn getColumn( final String columnName ) {
    return getInstance();
  }

  @Override
  public IColumn getColumn( final ColumnType type ) {
    return NullColumn.getInstance();
  }

  @Override
  public void setDefaultCell( final ICell defaultCell ) {
    this.defaultCell = defaultCell;
  }

  @Override
  public int size() {
    return rowCount;
  }

  @Override
  public IField getSchema() throws IOException {
    return null;
  }

  @Override
  public IField getSchema( final String schemaName ) throws IOException {
    return null;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append( String.format( "Column name : %s\n" , getColumnName() ) );
    result.append( String.format( "Column type : %s\n" , getColumnType() ) );
    result.append( "--------------------------\n" );
    IntStream.range( 0 , size() )
        .forEach( i -> {
          result.append( String.format( "CELL-%d: %s\n" , i , get( i ).toString() ) );
        } );

    return result.toString();
  }

}
