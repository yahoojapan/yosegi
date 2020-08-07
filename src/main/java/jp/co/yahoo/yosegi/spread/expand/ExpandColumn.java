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

package jp.co.yahoo.yosegi.spread.expand;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.ICellManager;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.ListIndexExpressionIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExpandColumn implements IColumn {

  private final IColumn original;
  private int[] columnIndexArray;

  public ExpandColumn( final IColumn original , final int[] columnIndexArray ) {
    this.original = original;
    this.columnIndexArray = columnIndexArray;
  }

  public int[] getColumnIndexArray() {
    return columnIndexArray;
  }

  @Override
  public void setColumnName( final String columnName ) {
    original.setColumnName( columnName );
  }

  @Override
  public String getColumnName() {
    return original.getColumnName();
  }

  @Override
  public ColumnType getColumnType() {
    return original.getColumnType();
  }

  @Override
  public void setParentsColumn( final IColumn column ) {
    original.setParentsColumn( column );
  }

  @Override
  public IColumn getParentsColumn() {
    return original.getParentsColumn();
  }

  @Override
  public int add( final ColumnType type , final Object obj , final int index ) throws IOException {
    throw new UnsupportedOperationException( "Expand column is read only." );
  }

  @Override
  public void addCell(
      final ColumnType type , final ICell obj , final int index ) throws IOException {
    throw new UnsupportedOperationException( "Expand column is read only." );
  }

  @Override
  public ICellManager getCellManager() {
    return null;
  }

  @Override
  public void setCellManager( final ICellManager cellManager ) {
    throw new UnsupportedOperationException( "Expand column is read only." );
  }

  @Override
  public ICell get( final int index ) {
    return original.get( columnIndexArray[index] );
  }

  @Override
  public List<String> getColumnKeys() {
    return original.getColumnKeys();
  }

  @Override
  public int getColumnSize() {
    return original.getColumnSize();
  }

  @Override
  public List<IColumn> getListColumn() {
    List<IColumn> convertList = new ArrayList<IColumn>();
    for ( IColumn column : original.getListColumn() ) {
      convertList.add( new ExpandColumn( column , columnIndexArray ) );
    }
    return convertList;
  }

  @Override
  public IColumn getColumn( final int index ) {
    if ( original.getColumnType() == ColumnType.ARRAY ) {
      return original.getColumn( index );
    }
    IColumn column = original.getColumn( index );
    return new ExpandColumn( column , columnIndexArray );
  }

  @Override
  public IColumn getColumn( final String columnName ) {
    if ( original.getColumnType() == ColumnType.ARRAY ) {
      return original.getColumn( columnName );
    }
    IColumn column = original.getColumn( columnName );
    return new ExpandColumn( column , columnIndexArray );
  }

  @Override
  public IColumn getColumn( final ColumnType type ) {
    if ( original.getColumnType() == ColumnType.ARRAY ) {
      return original.getColumn( type );
    }
    IColumn column = original.getColumn( type );
    return new ExpandColumn( column , columnIndexArray );
  }

  @Override
  public void setDefaultCell( final ICell defaultCell ) {
    original.setDefaultCell( defaultCell );
  }

  @Override
  public int size() {
    return columnIndexArray.length;
  }

  @Override
  public IField getSchema() throws IOException {
    return original.getSchema();
  }

  @Override
  public IField getSchema( final String schemaName ) throws IOException {
    return original.getSchema( schemaName );
  }

  @Override
  public void setIndex( final ICellIndex index ) {
    original.setIndex( index );
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException {
    boolean[] searchResult = original.filter( filter , new boolean[original.size()] );
    if ( searchResult == null ) {
      return null;
    }

    int index = 0;
    for ( ; index < filterArray.length 
        && columnIndexArray[index] < searchResult.length ; index++ ) {
      if ( searchResult[ columnIndexArray[index] ] ) {
        filterArray[index] = true;
      }
    }
    for ( ; index < filterArray.length ; index++ ) {
      filterArray[index] = true;
    }
    return filterArray;
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList , final int start , final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];

    List<Integer> originalIndexList = new ArrayList<Integer>();
    int maxIndex = -1;
    for ( int i = start ; i < ( start + length ) ; i++ ) {
      int target = indexList.get( i );
      if ( maxIndex < columnIndexArray[target] ) {
        originalIndexList.add( columnIndexArray[target] );
        maxIndex = columnIndexArray[target];
      }
    }
    PrimitiveObject[] originalResult = original.getPrimitiveObjectArray(
        new ListIndexExpressionIndex( originalIndexList ) , 0 , originalIndexList.size() );
    maxIndex = -1;
    int originalArrayIndex = -1;
    for ( int i = start,index = 0  ; i < ( start + length ) ; i++,index++ ) {
      int target = indexList.get( i );
      if ( maxIndex < columnIndexArray[target] ) {
        originalArrayIndex++;
        maxIndex = columnIndexArray[target];
      }
      result[index] = originalResult[originalArrayIndex];
    }
    return result;
  }

  @Override
  public void setPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ,
      final IMemoryAllocator allocator ) throws IOException {
    List<Integer> originalIndexList = new ArrayList<Integer>();
    int maxIndex = -1;
    for ( int i = start ; i < ( start + length ) ; i++ ) {
      int target = indexList.get( i );
      if ( maxIndex < columnIndexArray[target] ) {
        originalIndexList.add( columnIndexArray[target] );
        maxIndex = columnIndexArray[target];
      }
    }
    PrimitiveObject[] originalResult = original.getPrimitiveObjectArray(
        new ListIndexExpressionIndex( originalIndexList ) , 0 , originalIndexList.size() );
    maxIndex = -1;
    int originalArrayIndex = -1;
    for ( int i = start,index = 0  ; i < ( start + length ) ; i++,index++ ) {
      int target = indexList.get( i );
      if ( maxIndex < columnIndexArray[target] ) {
        originalArrayIndex++;
        maxIndex = columnIndexArray[target];
      }
      try {
        if ( originalResult[originalArrayIndex] == null ) {
          allocator.setNull( index );
        } else {
          allocator.setPrimitiveObject( index , originalResult[originalArrayIndex] );
        }
      } catch ( IOException ex ) {
        throw new RuntimeException( ex );
      }
    }
  }

  @Override
  public boolean isExpandColumn() {
    return true;
  }

  @Override
  public IColumn getInnerColumn() {
    return original;
  }

  @Override
  public int[] getExpandIndexArray() {
    return columnIndexArray;
  }

}
