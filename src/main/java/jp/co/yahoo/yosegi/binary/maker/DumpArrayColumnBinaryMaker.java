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
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ArrayCell;
import jp.co.yahoo.yosegi.spread.column.ArrayColumn;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.ICellManager;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.SpreadArrayLink;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class DumpArrayColumnBinaryMaker implements IColumnBinaryMaker {

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    byte[] binaryRaw = new byte[ Integer.BYTES * column.size() ];
    IntBuffer intIndexBuffer = ByteBuffer.wrap( binaryRaw ).asIntBuffer();
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell instanceof ArrayCell ) {
        ArrayCell arrayCell = (ArrayCell) cell;
        intIndexBuffer.put( arrayCell.getEnd() - arrayCell.getStart() );
      } else {
        intIndexBuffer.put( 0 );
      }
    }

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressData = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    IColumn childColumn = column.getColumn( 0 );
    List<ColumnBinary> columnBinaryList = new ArrayList<>();

    ColumnBinaryMakerCustomConfigNode childColumnConfigNode = null;
    IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
    if ( currentConfigNode != null ) {
      childColumnConfigNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
      if ( childColumnConfigNode != null ) {
        maker = childColumnConfigNode
            .getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
      }
    }
    columnBinaryList.add( maker.toBinary(
        commonConfig ,
        childColumnConfigNode ,
        compressResultNode.getChild( childColumn.getColumnName() ) ,
        childColumn ) );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.ARRAY ,
        column.size() ,
        binaryRaw.length ,
        0 ,
        -1 ,
        compressData ,
        0 ,
        compressData.length ,
        columnBinaryList );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return Integer.BYTES * analizeResult.getColumnSize();
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    return new LazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new ArrayColumnManager( columnBinary ) );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
      IMemoryAllocator childMemoryAllocator = allocator.getArrayChild(
          childColumnBinary.rowCount , childColumnBinary.columnType );
      maker.loadInMemoryStorage( childColumnBinary , childMemoryAllocator );
    }

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] decompressBuffer = compressor.decompress(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

    IntBuffer buffer = ByteBuffer.wrap( decompressBuffer ).asIntBuffer();
    int length = buffer.capacity();
    int currentIndex = 0;
    for ( int i = 0 ; i < length ; i++ ) {
      int arrayLength = buffer.get();
      if ( arrayLength == 0 ) {
        allocator.setNull(i);
      } else {
        int start = currentIndex;
        allocator.setArrayIndex( i , start , arrayLength );
        currentIndex += arrayLength;
      }
    }
    allocator.setValueCount( length );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class ArrayCellManager implements ICellManager<ICell> {

    private final ICell[] cellArray;

    /**
     * Manage it as an Array cell.
     */
    public ArrayCellManager( final Spread spread , final IntBuffer buffer ) {
      int length = buffer.capacity();
      cellArray = new ICell[length];
      int currentIndex = 0;
      for ( int i = 0 ; i < length ; i++ ) {
        int arrayLength = buffer.get();
        if ( arrayLength != 0 ) {
          int start = currentIndex;
          int end = start + arrayLength;
          cellArray[i] = new ArrayCell( new SpreadArrayLink( spread , i , start , end ) );
          currentIndex += arrayLength;
        }
      }
    }

    @Override
    public void add( final ICell cell , final int index ) {
      throw new UnsupportedOperationException( "read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ) {
      if ( cellArray.length <= index ) {
        return defaultCell;
      }

      if ( cellArray[index] == null ) {
        return defaultCell;
      }
      return cellArray[index];
    }

    @Override
    public int size() {
      return cellArray.length;
    }

    @Override
    public void clear() {
      // Do nothing
    }

    @Override
    public void setIndex( final ICellIndex index ) {
      // Do nothing
    }

    @Override
    public boolean[] filter(
        final IFilter filter , final boolean[] filterArray ) throws IOException {
      switch ( filter.getFilterType() ) {
        case NOT_NULL:
        case NULL:
        default:
          return null;
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveObjectArray(
        final IExpressionIndex indexList ,
        final int start ,
        final int length ) {
      return new PrimitiveObject[length];
    }

    @Override
    public void setPrimitiveObjectArray(
        final IExpressionIndex indexList ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ) {
      // Do nothing
    }

  }

  public class ArrayColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private ArrayColumn arrayColumn;
    private boolean isCreate;

    public ArrayColumnManager( final ColumnBinary columnBinary ) {
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      arrayColumn = new ArrayColumn( columnBinary.columnName );
      Spread spread = new Spread( arrayColumn );
      for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
        IColumn column = maker.toColumn( childColumnBinary );
        column.setParentsColumn( arrayColumn );
        spread.addColumn( column );
      }
      spread.setRowCount( columnBinary.rowCount );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] decompressBuffer = compressor.decompress(
          columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

      IntBuffer wrapBuffer = ByteBuffer.wrap( decompressBuffer ).asIntBuffer();
      arrayColumn.setSpread( spread );
      arrayColumn.setCellManager( new ArrayCellManager( spread , wrapBuffer ) );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      if ( ! isCreate ) {
        try {
          create();
        } catch ( IOException ex ) {
          throw new UncheckedIOException( ex );
        }
      }
      return arrayColumn;
    }

    @Override
    public List<String> getColumnKeys() {
      if ( isCreate ) {
        return arrayColumn.getColumnKeys();
      } else {
        return new ArrayList<>();
      }
    }

    @Override
    public int getColumnSize() {
      if ( isCreate ) {
        return arrayColumn.getColumnSize();
      } else {
        return 1;
      }
    }
  }

}
