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
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import java.io.IOException;
import java.util.List;

public interface IColumn {

  void setColumnName( final String columnName );

  String getColumnName();

  ColumnType getColumnType();

  void setParentsColumn( final IColumn column );

  IColumn getParentsColumn();

  int add( final ColumnType type , final Object obj , final int index ) throws IOException;

  void addCell( final ColumnType type , final ICell obj , final int index ) throws IOException;

  ICellManager getCellManager();

  void setCellManager( final ICellManager cellManager );

  ICell get( final int index );

  List<String> getColumnKeys();

  int getColumnSize();

  List<IColumn> getListColumn();

  IColumn getColumn( final int index );

  IColumn getColumn( final String columnName );

  IColumn getColumn( final ColumnType type );

  void setDefaultCell( final ICell defaultCell );

  int size();

  IField getSchema() throws IOException;

  IField getSchema( final String schemaName ) throws IOException;

  void setIndex( final ICellIndex index );

  boolean[] filter( final IFilter filter , boolean[] filterArray ) throws IOException;

  PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList , final int start , final int length );

  void setPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ,
      final IMemoryAllocator allocator ) throws IOException;

}
