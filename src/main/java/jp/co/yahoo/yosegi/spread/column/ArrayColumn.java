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

import jp.co.yahoo.yosegi.constants.PrimitiveByteLength;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.NullField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;
import jp.co.yahoo.yosegi.spread.expression.ListIndexExpressionIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrayColumn implements IColumn {

  private static final String CHILD_COLUMN_NAME = ColumnType.ARRAY.toString();

  private String columnName;
  private ICellManager cellManager;
  private Spread spread;
  private IColumn parentsColumn;
  private ICell defaultCell;

  /**
   * Initialized by setting the column name.
   */
  public ArrayColumn( final String columnName ) {
    this.columnName = columnName;
    spread = new Spread( this );
    cellManager = new CellManager();
    defaultCell = EmptyArrayCell.getInstance();
    parentsColumn = NullColumn.getInstance();
  }

  public void setSpread( final Spread spread ) {
    this.spread = spread;
    cellManager.clear();
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
    return ColumnType.ARRAY;
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
    if ( type != getColumnType() ) {
      throw new IOException( "Incorrect input data type : " + obj.getClass().getName() );
    }
    int totalBytes = 0;
    int start = spread.size();
    if ( obj instanceof List ) {
      for ( Object childObject : (List) obj ) {
        totalBytes += spread.addRow( CHILD_COLUMN_NAME , childObject );
      }
    } else {
      IParser parser = (IParser)obj;
      boolean hasParser = parser.hasParser(0);
      for ( int i = 0 ; i < parser.size() ; i++ ) {
        if ( hasParser ) {
          totalBytes += spread.addRow( CHILD_COLUMN_NAME , parser.getParser(i) );
        } else {
          totalBytes += spread.addRow( CHILD_COLUMN_NAME , parser.get(i) );
        }
      }
    }
    int end = spread.size();

    cellManager.add( new ArrayCell( new SpreadArrayLink( spread , index , start , end ) ) , index );
    totalBytes += Integer.BYTES * ( end - start );
    totalBytes += PrimitiveByteLength.JAVA_OBJECT_LENGTH * ( end - start );
    return totalBytes;
  }

  @Override
  public void addCell(
      final ColumnType type , final ICell cell , final int index ) throws IOException {
    cellManager.add( cell , index );
  }

  @Override
  public ICellManager getCellManager() {
    return cellManager;
  }

  @Override
  public void setCellManager( final ICellManager cellManager ) {
    this.cellManager = cellManager;
  }

  @Override
  public ICell get( final int index ) {
    return cellManager.get( index , defaultCell );
  }

  @Override
  public List<String> getColumnKeys() {
    return new ArrayList<String>();
  }

  @Override
  public int getColumnSize() {
    return spread.getColumnSize();
  }

  @Override
  public List<IColumn> getListColumn() {
    return spread.getListColumn();
  }

  @Override
  public IColumn getColumn( final int index ) {
    if ( index != 0 ) {
      return NullColumn.getInstance();
    }
    return spread.getColumn( 0 );
  }

  @Override
  public IColumn getColumn( final String columnName ) {
    return NullColumn.getInstance();
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
    return cellManager.size();
  }

  @Override
  public IField getSchema() throws IOException {
    return getSchema( getColumnName() );
  }

  @Override
  public IField getSchema( final String schemaName ) throws IOException {
    IField childSchema = spread.getColumn(0).getSchema();
    if ( childSchema == null ) {
      childSchema = new NullField( "dummy" );
    }
    return new ArrayContainerField( schemaName , childSchema );
  }

  public IColumn getChildColumn() {
    return spread.getColumn(0);
  }

  @Override
  public void setIndex( final ICellIndex index ) {
    cellManager.setIndex( index );
  }

  @Override
  public boolean[] filter( final IFilter filter , final boolean[] filterArray ) throws IOException {
    return cellManager.filter( filter , filterArray );
  }

  @Override
  public PrimitiveObject[] getPrimitiveObjectArray(
      final IExpressionIndex indexList , final int start , final int length ) {
    PrimitiveObject[] result = new PrimitiveObject[length];
    return result;
  }

  @Override
  public void setPrimitiveObjectArray(
      final IExpressionIndex indexList ,
      final int start ,
      final int length ,
      final IMemoryAllocator allocator ) throws IOException {
    allocator.setValueCount( length );
    List<Integer> childIndexList = new ArrayList<Integer>();
    for ( int i = start ; i < start + length ; i++ ) {
      int index = indexList.get( i );
      ICell cell = cellManager.get( index , EmptyArrayCell.getInstance() );
      if ( cell.getType() == ColumnType.EMPTY_ARRAY ) {
        allocator.setNull( index );
        continue;
      }
      ArrayCell arrayCell = (ArrayCell)cell;
      for ( int ii = arrayCell.getStart() ; ii < arrayCell.getEnd() ; ii++ ) {
        childIndexList.add( Integer.valueOf( ii ) );
      }
    }
    ListIndexExpressionIndex newIndexList = new ListIndexExpressionIndex( childIndexList );
    IColumn column = spread.getColumn(0);
    IMemoryAllocator childAllocator =
        allocator.getArrayChild( newIndexList.size() , column.getColumnType() );
    column.setPrimitiveObjectArray( newIndexList , 0 , newIndexList.size() , childAllocator );
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append( String.format( "Column name : %s\n" , getColumnName() ) );
    result.append( String.format( "Column type : %s<" , getColumnType() ) );
    result.append( IntStream.range( 0 , spread.getColumnSize() )
        .mapToObj( i -> spread.getColumn(i).getColumnType().toString() )
        .collect( Collectors.joining( "," ) ) );
    result.append( ">\n" );
    result.append( "--------------------------\n" );
    IntStream.range( 0 , size() )
        .forEach( i -> {
          result.append( String.format( "CELL-%d: %s\n" , i , get( i ).toString() ) );
        } );

    return result.toString();
  }

  public static class EmptyArrayCell implements ICell<Object,List<ICell>> {

    private static final EmptyArrayCell CELL = new EmptyArrayCell();
    private static final List<ICell> RESULT = new ArrayList<ICell>();

    private EmptyArrayCell() {}

    public static ICell getInstance() {
      return CELL;
    }

    @Override
    public List<ICell> getRow() {
      return RESULT;
    }

    @Override
    public void setRow( final Object object ) {
    }

    @Override
    public ColumnType getType() {
      return ColumnType.EMPTY_ARRAY;
    }

    @Override
    public String toString() {
      StringBuffer result = new StringBuffer();
      result.append( String.format( "(%s)" , getType() ) );
      result.append( "[]" );

      return RESULT.toString();
    }

  }

}
