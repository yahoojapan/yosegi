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
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrowStructColumn implements IColumn {

  private final String columnName;
  private final Spread spread;

  private ICell defaultCell = SpreadColumn.EmptySpreadCell.getInstance();
  private IColumn parentColumn = NullColumn.getInstance();

  /**
   * Initialize by setting column name and Arrow vector.
   */
  public ArrowStructColumn( final String columnName , final StructVector vector ) {
    this.columnName = columnName;

    List<FieldVector> vectorList = vector.getChildrenFromFields();
    spread = new Spread();
    for ( FieldVector v : vectorList ) {
      spread.addColumn( ArrowColumnFactory.convert( v.getField().getName() , v ) );
    }
    spread.setRowCount( vector.getValueCount() );
  }

  @Override
  public void setColumnName( final String columnName ) {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public ColumnType getColumnType() {
    return ColumnType.SPREAD;
  }

  @Override
  public void setParentsColumn( final IColumn column ) {
    this.parentColumn = parentColumn;
  }

  @Override
  public IColumn getParentsColumn() {
    return parentColumn;
  }

  @Override
  public int add(
      final ColumnType type , final Object obj , final int index ) throws IOException {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public void addCell(
      final ColumnType type , final ICell obj , final int index ) throws IOException {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public ICellManager getCellManager() {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public void setCellManager( final ICellManager cellManager ) {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public ICell get( final int index ) {
    return new SpreadCell( new SpreadLink( spread , index ) );
  }

  @Override
  public List<String> getColumnKeys() {
    return spread.getColumnKeys();
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
    return spread.getColumn( index );
  }

  @Override
  public IColumn getColumn( final String columnName ) {
    return spread.getColumn( columnName );
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
    return spread.size();
  }

  @Override
  public IField getSchema() throws IOException {
    return getSchema( getColumnName() );
  }

  @Override
  public IField getSchema( final String schemaName ) throws IOException {
    StructContainerField schema = new StructContainerField( schemaName );
    IntStream.range( 0 , spread.getColumnSize() )
        .forEach( i -> {
          try {
            schema.set( spread.getColumn(i).getSchema() );
          } catch ( IOException ex ) {
            throw new UncheckedIOException( "IOException addRow in lambda." , ex );
          }
        } );
    return schema;
  }

  @Override
  public void setIndex( final ICellIndex index ) {
    throw new UnsupportedOperationException( "This column is read only." );
  }

  @Override
  public boolean[] filter( final IFilter filter , boolean[] filterArray ) throws IOException {
    throw new UnsupportedOperationException( "This column is read only." );
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
    for ( IColumn column : spread.getListColumn() ) {
      IMemoryAllocator childAllocator =
          allocator.getChild( column.getColumnName() , column.getColumnType() );
      column.setPrimitiveObjectArray( indexList , start , length , childAllocator );
    }
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    result.append( String.format( "Column name : %s\n" , getColumnName() ) );
    result.append( String.format( "Column type : %s<" , getColumnType() ) );
    result.append( IntStream.range( 0 , spread.getColumnSize() )
        .mapToObj( i -> {
          IColumn column = spread.getColumn(i);
          return String.format( "%s %s" , column.getColumnName() , column.getColumnType() );
        } )
        .collect( Collectors.joining( "," ) ) );
    result.append( ">\n" );
    result.append( "--------------------------\n" );
    IntStream.range( 0 , size() )
        .forEach( i -> {
          result.append( String.format( "CELL-%d: %s\n" , i , get( i ).toString() ) );
        } );

    return result.toString();
  }

}
