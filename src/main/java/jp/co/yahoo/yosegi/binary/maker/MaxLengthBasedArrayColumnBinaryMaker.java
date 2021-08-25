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
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

public class MaxLengthBasedArrayColumnBinaryMaker implements IColumnBinaryMaker {

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

    int maxSize = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell instanceof ArrayCell ) {
        ArrayCell arrayCell = (ArrayCell) cell;
        int arrayLength = arrayCell.getEnd() - arrayCell.getStart();
        if ( maxSize < arrayLength  ) {
          maxSize = arrayLength;
        }
      }
    }
    NumberToBinaryUtils.IIntConverter encoder = NumberToBinaryUtils.getIntConverter( 0 , maxSize );
    byte[] binaryRaw = new byte[Integer.BYTES + encoder.calcBinarySize( column.size() )];
    ByteBuffer.wrap( binaryRaw ).putInt( maxSize );
    IWriteSupporter writer = encoder.toWriteSuppoter(
        column.size() , binaryRaw , Integer.BYTES , encoder.calcBinarySize( column.size() ) );

    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell instanceof ArrayCell ) {
        ArrayCell arrayCell = (ArrayCell) cell;
        writer.putInt( arrayCell.getEnd() - arrayCell.getStart() );
      } else {
        writer.putInt( 0 );
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
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();

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
    int maxSize = ByteBuffer.wrap( decompressBuffer ).getInt();
    NumberToBinaryUtils.IIntConverter encoder = NumberToBinaryUtils.getIntConverter( 0 , maxSize );
    IReadSupporter reader = encoder.toReadSupporter(
        decompressBuffer , Integer.BYTES , decompressBuffer.length - Integer.BYTES );

    allocator.setValueCount( columnBinary.rowCount );
    int currentIndex = 0;
    for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
      int arrayLength = reader.getInt();
      if ( arrayLength == 0 ) {
        allocator.setNull(i);
      } else {
        int start = currentIndex;
        allocator.setArrayIndex( i , start , arrayLength );
        currentIndex += arrayLength;
      }
    }
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    if ( columnBinary.columnBinaryList.isEmpty() ) {
      parentNode.getChildNode( columnBinary.columnName ).disable();
      return;
    }
    ColumnBinary childColumnBinary = columnBinary.columnBinaryList.get(0);
    IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
    if ( parentNode.containsKey( columnBinary.columnName ) ) {
      parentNode.putChildNode( 
          childColumnBinary.columnName , parentNode.getChildNode( columnBinary.columnName ) );

    }
    maker.setBlockIndexNode( parentNode , childColumnBinary , spreadIndex );
    parentNode.putChildNode( 
        columnBinary.columnName , parentNode.getChildNode( childColumnBinary.columnName ) );
    parentNode.deleteChildNode( childColumnBinary.columnName );
  }

  public class ArrayCellManager implements ICellManager<ICell> {

    private final ICell[] cellArray;

    /**
     * Manage it as an Array cell.
     */
    public ArrayCellManager( final ICell[] cellArray ) {
      this.cellArray = cellArray;
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
    public void clear() {}

    @Override
    public PrimitiveObject[] getPrimitiveObjectArray(
        final int start ,
        final int length ) {
      return new PrimitiveObject[length];
    }

    @Override
    public void setPrimitiveObjectArray(
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ) {
      return;
    }

  }

  public class ArrayColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private ArrayColumn arrayColumn;
    private boolean isCreate;

    public ArrayColumnManager( final ColumnBinary columnBinary ) throws IOException {
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
      int maxSize = ByteBuffer.wrap( decompressBuffer ).getInt();
      NumberToBinaryUtils.IIntConverter encoder =
          NumberToBinaryUtils.getIntConverter( 0 , maxSize );
      IReadSupporter reader = encoder.toReadSupporter(
          decompressBuffer , Integer.BYTES , decompressBuffer.length - Integer.BYTES );

      int currentIndex = 0;
      ICell[] arrayCells = new ICell[ columnBinary.rowCount ];
      for ( int i = 0 ; i < columnBinary.rowCount ; i++ ) {
        int arrayLength = reader.getInt();
        if ( arrayLength != 0 ) {
          int end = currentIndex + arrayLength;
          arrayCells[i] = new ArrayCell( new SpreadArrayLink( spread , i , currentIndex , end ) );
          currentIndex += arrayLength;
        }
      }

      arrayColumn.setSpread( spread );
      arrayColumn.setCellManager( new ArrayCellManager( arrayCells ) );

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
        return new ArrayList<String>();
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
