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

import java.io.IOException;
import java.util.List;

public class LazyColumn implements IColumn {

  private final ColumnType columnType;
  private final IColumnManager columnManager;
  private String columnName;
  private IColumn parentsColumn = NullColumn.getInstance();

  /**
   * Deserialize for the first time when there is access to the column value.
   */
  public LazyColumn(
      final String columnName ,
      final ColumnType columnType ,
      final IColumnManager columnManager ) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.columnManager = columnManager;
  }

  @Override
  public void setColumnName( final String columnName ) {
    this.columnName = columnName;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public ColumnType getColumnType() {
    return columnType;
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
  public int add(
      final ColumnType type , final Object obj , final int index ) throws IOException {
    return columnManager.get().add( type , obj , index );
  }

  @Override
  public void addCell(
      final ColumnType type , final ICell cell , final int index ) throws IOException {
    columnManager.get().addCell( type , cell , index );
  }

  @Override
  public ICellManager getCellManager() {
    return columnManager.get().getCellManager();
  }

  @Override
  public void setCellManager( final ICellManager cellManager ) {
    columnManager.get().setCellManager( cellManager );
  }

  @Override
  public ICell get( final int index ) {
    return columnManager.get().get( index );
  }

  @Override
  public List<String> getColumnKeys() {
    return columnManager.getColumnKeys();
  }

  @Override
  public int getColumnSize() {
    return columnManager.getColumnSize();
  }

  @Override
  public List<IColumn> getListColumn() {
    return columnManager.get().getListColumn();
  }

  @Override
  public IColumn getColumn( final int index ) {
    return columnManager.get().getColumn( index );
  }

  @Override
  public IColumn getColumn( final String columnName ) {
    return columnManager.get().getColumn( columnName );
  }

  @Override
  public IColumn getColumn( final ColumnType type ) {
    return columnManager.get().getColumn( type );
  }

  @Override
  public void setDefaultCell( final ICell defaultCell ) {
    columnManager.get().setDefaultCell( defaultCell );
  }

  @Override
  public int size() {
    return columnManager.get().size();
  }

  @Override
  public IField getSchema() throws IOException {
    return columnManager.get().getSchema( getColumnName() );
  }

  @Override
  public IField getSchema( final String schemaName ) throws IOException {
    return columnManager.get().getSchema( schemaName );
  }

  @Override
  public String toString() {
    return columnManager.get().toString();
  }

}
