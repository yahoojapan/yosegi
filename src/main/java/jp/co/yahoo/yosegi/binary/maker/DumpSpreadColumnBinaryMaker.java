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

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.DefaultCompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadColumn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class DumpSpreadColumnBinaryMaker implements IColumnBinaryMaker {

  /**
   * Create a new Spread ColumnBinary.
   */
  public static ColumnBinary createSpreadColumnBinary(
      final String columnName ,
      final int columnSize ,
      final List<ColumnBinary> childList ) {
    return new ColumnBinary(
        DumpSpreadColumnBinaryMaker.class.getName() ,
        DefaultCompressor.class.getName() ,
        columnName ,
        ColumnType.SPREAD ,
        columnSize ,
        0 ,
        0 ,
        -1 ,
        new byte[0] ,
        0 ,
        0 ,
        childList );
  }

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    List<IColumn> childColumnList = column.getListColumn();
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();
    for ( IColumn childColumn : childColumnList ) {
      ColumnBinaryMakerCustomConfigNode childNode = null;
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
      if ( currentConfigNode != null ) {
        childNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
        if ( childNode != null ) {
          maker = childNode.getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
        }
      }
      columnBinaryList.add( maker.toBinary( commonConfig , childNode , childColumn ) );
    }

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.SPREAD ,
        column.size() ,
        0 ,
        0 ,
        -1 ,
        new byte[0] ,
        0 ,
        0 ,
        columnBinaryList );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return 0;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    return new LazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new SpreadColumnManager( columnBinary ) );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    int maxValueCount = 0;
    allocator.setChildCount( columnBinary.columnBinaryList.size() );
    for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
      IMemoryAllocator childAllocator =
          allocator.getChild( childColumnBinary.columnName , childColumnBinary.columnType );
      maker.loadInMemoryStorage( childColumnBinary , childAllocator );
      if ( maxValueCount < childAllocator.getValueCount() ) {
        maxValueCount = childAllocator.getValueCount();
      }
    }
    allocator.setValueCount( maxValueCount );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class SpreadColumnManager implements IColumnManager {

    private final List<String> keyList;
    private final ColumnBinary columnBinary;
    private SpreadColumn spreadColumn;
    private boolean isCreate;

    /**
     * Create a SpreadColumn from ColumnBinary.
     */
    public SpreadColumnManager( final ColumnBinary columnBinary ) throws IOException {
      this.columnBinary = columnBinary;
      keyList = new ArrayList<String>();
      for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
        keyList.add( childColumnBinary.columnName );
      }
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }

      spreadColumn = new SpreadColumn( columnBinary.columnName );
      Spread spread = new Spread();
      for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
        IColumnBinaryMaker maker =
            FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
        IColumn column = maker.toColumn( childColumnBinary );
        column.setParentsColumn( spreadColumn );
        spread.addColumn( column );
      }
      spread.setRowCount( columnBinary.rowCount );

      spreadColumn.setSpread( spread );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      try {
        create();
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
      }
      return spreadColumn;
    }

    @Override
    public List<String> getColumnKeys() {
      if ( isCreate ) {
        return spreadColumn.getColumnKeys();
      } else {
        return keyList;
      }
    }

    @Override
    public int getColumnSize() {
      if ( isCreate ) {
        return spreadColumn.getColumnSize();
      } else {
        return keyList.size();
      }
    }
  }

}
