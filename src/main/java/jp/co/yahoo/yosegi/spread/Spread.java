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

package jp.co.yahoo.yosegi.spread;

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.spread.column.ColumnFactory;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ColumnTypeFactory;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.NullColumn;
import jp.co.yahoo.yosegi.spread.column.UnionColumn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class Spread {

  private final Map<String,Integer> columnIndexMapping = new HashMap<String,Integer>();
  private final List<IColumn> columnList = new ArrayList<IColumn>();
  private final IColumn parentColumn;
  private int rowCount;

  public Spread() {
    parentColumn = NullColumn.getInstance();
  }

  public Spread( final IColumn parentColumn ) {
    this.parentColumn = parentColumn;
  }

  /**
   * Get the object in the specified line.
   */
  public Map<String,ICell> getLine( final Map<String,ICell> previous , final int index ) {
    Map<String,ICell> result = previous;
    if ( result == null ) {
      result = new HashMap<String,ICell>( columnList.size() );
    } else {
      result.clear();
    }

    for ( IColumn column : columnList ) {
      result.put( column.getColumnName() , column.get( index ) );
    }

    return result;
  }

  private int registerRow( final String columnName , final Object row ) throws IOException {
    ColumnType type = ColumnTypeFactory.get( row );
    switch ( type ) {
      case EMPTY_SPREAD:
      case EMPTY_ARRAY:
      case NULL:
        return 0;
      default:
    }

    int index = getColumnIndex( columnName );

    if ( index == -1 ) {
      IColumn column = ColumnFactory.get( type , columnName );
      column.setParentsColumn( parentColumn );
      columnIndexMapping.put( columnName , Integer.valueOf( columnList.size() ) );
      index = columnList.size();
      columnList.add( column );
    }
    IColumn column = columnList.get( index );
    if ( column.getColumnType() != ColumnType.UNION && column.getColumnType() != type ) {
      UnionColumn unionColumn = new UnionColumn( column );
      unionColumn.setParentsColumn( parentColumn );
      columnList.set( index , unionColumn );
      column = unionColumn;
    }
    return column.add( type , row , rowCount );
  }

  /**
   * Add row data.
   */
  public int addRow( final String key , final Object row ) throws IOException {
    int totalBytes = registerRow( key , row );
    rowCount++;
    totalBytes += getColumnSize() * 4;
    return totalBytes;
  }

  /**
   * Add row data.
   */
  public int addRow( final Map<String,Object> row ) throws IOException {
    int totalBytes = 0;
    for ( Map.Entry<String,Object> entry : row.entrySet() ) {
      totalBytes += registerRow( entry.getKey() , entry.getValue() );
    }
    totalBytes += getColumnSize() * 4;
    rowCount++;
    return totalBytes;
  }

  /**
   * Add row data.
   */
  public int addParserRow( final IParser parser )throws IOException {
    String[] keys = parser.getAllKey();
    int totalBytes = 0;
    for ( String key : keys ) {
      if ( parser.hasParser( key ) ) {
        totalBytes += registerRow( key , parser.getParser( key ) );
      } else {
        totalBytes += registerRow( key , parser.get(key) );
      }
    }
    rowCount++;
    return totalBytes;
  }

  /**
   * Add rows data.
   */
  public int addRows( final List<Map<String,Object>> rows ) throws IOException {
    int totalBytes = 0;
    for ( Map<String,Object> row : rows ) {
      totalBytes += addRow( row );
    }
    return totalBytes;
  }

  public List<IColumn> getListColumn() {
    return columnList;
  }

  /**
   * Get all columns.
   */
  public Map<String,IColumn> getAllColumn() {
    Map<String,IColumn> result = new HashMap<String,IColumn>();
    columnList.stream().forEach( column -> result.put( column.getColumnName() , column ) );
    return result;
  }

  public List<String> getColumnKeys() {
    return new ArrayList<String>( columnIndexMapping.keySet() );
  }

  /**
   * Column is acquired from column number.
   */
  public IColumn getColumn( final int index ) {
    if ( columnList.size() <= index ) {
      return NullColumn.getInstance();
    }
    return columnList.get( index );
  }

  /**
   * Gets a column from the column name.
   */
  public IColumn getColumn( final String columnName ) {
    if ( ! containsColumn( columnName ) ) {
      return NullColumn.getInstance();
    }

    return getColumn( columnIndexMapping.get( columnName ).intValue() );
  }

  public void addColumn( final IColumn column ) {
    columnIndexMapping.put( column.getColumnName() , Integer.valueOf( columnList.size() ) );
    columnList.add( column );
  }

  public boolean containsColumn( final String columnName ) {
    return columnIndexMapping.containsKey( columnName );
  }

  /**
   * Acquire column index number from column name.
   */
  public int getColumnIndex( final String columnName ) {
    if ( ! containsColumn( columnName ) ) {
      return -1;
    }

    return columnIndexMapping.get( columnName ).intValue();
  }

  public void setRowCount( final int rowCount ) {
    this.rowCount = rowCount;
  }

  public int getColumnSize() {
    return columnList.size();
  }

  public int size() {
    return rowCount;
  }

  public IField getSchema() throws IOException {
    return getSchema( "root" );
  }

  /**
   * Create a schema from this column structure of Spread.
   */
  public IField getSchema( final String schemaName ) throws IOException {
    StructContainerField schema = new StructContainerField( schemaName );
    columnList.stream()
        .forEach( column -> {
          try {
            schema.set( column.getSchema() );
          } catch ( IOException ex ) {
            throw new UncheckedIOException( "IOException addRow in lambda." , ex );
          }
        } );
    return schema;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();
    Map<String,ICell> cache = new HashMap<String,ICell>();
    IntStream.range( 0 , rowCount )
        .forEach( i -> {
          Map<String,ICell> line = getLine( cache , i );
          result.append( "--------------------------\n" );
          result.append( String.format( "LINE-%d\n" , i ) );
          result.append( "--------------------------\n" );
          result.append( line.toString() );
          result.append( "\n" );
        } );

    return result.toString();
  }

}
