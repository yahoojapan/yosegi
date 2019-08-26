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
import jp.co.yahoo.yosegi.binary.maker.index.RangeLongIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.ByteColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IntegerColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.LongColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.ShortColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.util.DetermineMinMax;
import jp.co.yahoo.yosegi.util.DetermineMinMaxFactory;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;
import jp.co.yahoo.yosegi.util.io.diffencoder.INumEncoder;
import jp.co.yahoo.yosegi.util.io.diffencoder.NumEncoderUtil;
import jp.co.yahoo.yosegi.util.io.nullencoder.NullBinaryEncoder;
import jp.co.yahoo.yosegi.util.io.rle.RleConverter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class RleLongColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, rowCount, rowGroupCount ,
  //    maxRowGroupCount , nullIndexLength, lengthBinarySize
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 6;

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    if ( column.size() == 0 ) {
      return new UnsupportedColumnBinaryMaker()
          .toBinary( commonConfig , currentConfigNode , compressResultNode , column );
    }
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    boolean[] isNullArray = new boolean[column.size()];

    DetermineMinMax<Long> detemineMinMax = DetermineMinMaxFactory.createLong();
    int rowCount = 0;
    int nullCount = 0;
    int nullMaxIndex = 0;
    int notNullMaxIndex = 0;

    int startIndex = 0;
    for ( ; startIndex < column.size() ; startIndex++ ) {
      ICell cell = column.get(startIndex);
      if ( cell.getType() != ColumnType.NULL ) {
        break;
      }
    }

    RleConverter<Long> rleConverter = null;
    for ( int i = startIndex,arrayIndex = 0 ; i < column.size() ; i++,arrayIndex++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        nullMaxIndex = arrayIndex;
        isNullArray[arrayIndex] = true;
        continue;
      }
      notNullMaxIndex = arrayIndex;
      PrimitiveObject primitiveObj = ( (PrimitiveCell) cell).getRow();
      Long target = Long.valueOf( primitiveObj.getLong() );
      detemineMinMax.set( target );
      if ( rleConverter == null ) {
        rleConverter = new RleConverter<Long>( target , new Long[column.size()] );
      }
      rleConverter.add( target );
      rowCount++;
    }
    rleConverter.finish();

    ByteOrder order = ByteOrder.nativeOrder();
    int nullIndexLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    NumberToBinaryUtils.IIntConverter lengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , rleConverter.getMaxGroupLength() );
    int lengthBinaryLength = lengthEncoder.calcBinarySize( rleConverter.getRowGroupCount() );

    INumEncoder valueEncoder =
        NumEncoderUtil.createEncoder( detemineMinMax.getMin() , detemineMinMax.getMax() );
    int valueLength = valueEncoder.calcBinarySize( rleConverter.getRowGroupCount() );

    byte[] binaryRaw = new byte[ META_LENGTH + nullIndexLength + lengthBinaryLength + valueLength ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.put( order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1 );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( rowCount );
    wrapBuffer.putInt( rleConverter.getRowGroupCount() );
    wrapBuffer.putInt( rleConverter.getMaxGroupLength() );
    wrapBuffer.putInt( nullIndexLength );
    wrapBuffer.putInt( lengthBinaryLength );
    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH ,
        nullIndexLength ,
        isNullArray ,
        nullCount ,
        rowCount ,
        nullMaxIndex ,
        notNullMaxIndex );

    IWriteSupporter lengthWriter = lengthEncoder.toWriteSuppoter(
        rleConverter.getRowGroupCount() , 
        binaryRaw , 
        META_LENGTH + nullIndexLength , 
        lengthBinaryLength );
    int[] lengthArray = rleConverter.getLengthArray();
    for ( int i = 0 ; i < rleConverter.getRowGroupCount() ; i++ ) {
      lengthWriter.putInt( lengthArray[i] );
    }

    valueEncoder.toBinary(
        rleConverter.getValueArray() , 
        binaryRaw ,
        META_LENGTH + nullIndexLength + lengthBinaryLength,
        rleConverter.getRowGroupCount() ,
        order );

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary = new byte[ Long.BYTES * 2 + compressBinary.length ];

    wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putLong( detemineMinMax.getMin() );
    wrapBuffer.putLong( detemineMinMax.getMax() );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binaryRaw.length ,
        NumEncoderUtil.getLogicalSize( rowCount , column.getColumnType() ) ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    int startIndex = analizeResult.getRowStart();
    int maxIndex = analizeResult.getRowEnd();
    int nullCount = analizeResult.getNullCount() - startIndex;
    int notNullCount = analizeResult.getRowCount();
    int nullIgnoreRleRowGroupCount = analizeResult.getNullIgnoreRleGroupCount();
    int nullIgnoreRleMaxRowGroupLength = analizeResult.getNullIgonoreRleMaxRowGroupLength();

    int nullIndexLength =
        NullBinaryEncoder.getBinarySize( nullCount , notNullCount , maxIndex , maxIndex );

    long min;
    long max;
    switch ( analizeResult.getColumnType() ) {
      case BYTE:
        min = (long)( (ByteColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ByteColumnAnalizeResult) analizeResult ).getMax();
        break;
      case SHORT:
        min = (long)( (ShortColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (ShortColumnAnalizeResult) analizeResult ).getMax();
        break;
      case INTEGER:
        min = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMin();
        max = (long)( (IntegerColumnAnalizeResult) analizeResult ).getMax();
        break;
      case LONG:
        min = ( (LongColumnAnalizeResult) analizeResult ).getMin();
        max = ( (LongColumnAnalizeResult) analizeResult ).getMax();
        break;
      default:
        min = Long.MIN_VALUE;
        max = Long.MAX_VALUE;
        break;
    }
    NumberToBinaryUtils.IIntConverter lengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , nullIgnoreRleMaxRowGroupLength );
    int lengthBinaryLength = lengthEncoder.calcBinarySize( nullIgnoreRleRowGroupCount );

    int valueLength;
    try {
      INumEncoder valueEncoder =
          NumEncoderUtil.createEncoder( min , max );
      valueLength = valueEncoder.calcBinarySize( nullIgnoreRleRowGroupCount );
    } catch ( IOException ex ) {
      throw new UncheckedIOException( ex );
    } 

    return META_LENGTH + nullIndexLength + lengthBinaryLength + valueLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );

    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ColumnManager(
        min.longValue(),
        max.longValue(),
        columnBinary
      ) ,
      new RangeLongIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer compressWrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    long min = compressWrapBuffer.getLong();
    long max = compressWrapBuffer.getLong();

    int start = columnBinary.binaryStart + ( Long.BYTES * 2 );
    int length = columnBinary.binaryLength - ( Long.BYTES * 2 );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );

    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    int rowCount = wrapBuffer.getInt();
    int rowGroupCount = wrapBuffer.getInt();
    int maxRowGroupCount = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int lengthBinarySize = wrapBuffer.getInt();
    int valueBinaryLength = binary.length - META_LENGTH - nullIndexLength - lengthBinarySize;

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullIndexLength );

    allocator.setValueCount( startIndex + isNullArray.length );

    for ( int index = 0 ; index < startIndex ; index++ ) {
      allocator.setNull( index );
    }
    NumberToBinaryUtils.IIntConverter lengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
    IReadSupporter lengthReader = lengthEncoder.toReadSupporter(
        binary , 
        META_LENGTH + nullIndexLength , 
        lengthBinarySize );

    INumEncoder valueEncoder =
        NumEncoderUtil.createEncoder( min , max );
    long[] valueArray = valueEncoder.toLongArray(
        binary,
        META_LENGTH + nullIndexLength + lengthBinarySize,
        rowGroupCount,
        order );
    int index = 0;
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      int valueLength = lengthReader.getInt();
      for ( int n = 0 ; n < valueLength ; index++ ) {
        if ( isNullArray[index] ) {
          allocator.setNull( index + startIndex );
          continue;
        }
        allocator.setLong( index + startIndex , valueArray[i] );
        n++;
      }
    }
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Long min = Long.valueOf( wrapBuffer.getLong() );
    Long max = Long.valueOf( wrapBuffer.getLong() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new LongRangeBlockIndex( min , max ) );
  }

  public class ColumnManager implements IColumnManager {

    private final long min;
    private final long max;
    private final ColumnBinary columnBinary;

    private PrimitiveColumn column;
    private boolean isCreate;

    /**
     * Init.
     */
    public ColumnManager(
        final long min,
        final long max,
        final ColumnBinary columnBinary ) {
      this.min = min;
      this.max = max;
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      int start = columnBinary.binaryStart + ( Long.BYTES * 2 );
      int length = columnBinary.binaryLength - ( Long.BYTES * 2 );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );

      ByteOrder order = wrapBuffer.get() == (byte)0
          ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      int startIndex = wrapBuffer.getInt();
      int rowCount = wrapBuffer.getInt();
      int rowGroupCount = wrapBuffer.getInt();
      int maxRowGroupCount = wrapBuffer.getInt();
      int nullIndexLength = wrapBuffer.getInt();
      int lengthBinarySize = wrapBuffer.getInt();
      int valueBinaryLength =
          binary.length - META_LENGTH - nullIndexLength - lengthBinarySize;

      boolean[] isNullArray =
          NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullIndexLength );

      NumberToBinaryUtils.IIntConverter lengthEncoder =
          NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
      IReadSupporter lengthReader = lengthEncoder.toReadSupporter(
          binary ,
          META_LENGTH + nullIndexLength ,
          lengthBinarySize );

      INumEncoder valueEncoder =
          NumEncoderUtil.createEncoder( min , max );
      PrimitiveObject[] valueArray = valueEncoder.toPrimitiveArray(
          binary,
          META_LENGTH + nullIndexLength + lengthBinarySize,
          rowGroupCount,
          order );

      PrimitiveObject[] columnArray = new PrimitiveObject[isNullArray.length];
      int index = 0;
      for ( int i = 0 ; i < rowGroupCount ; i++ ) {
        int valueLength = lengthReader.getInt();
        for ( int n = 0 ; n < valueLength ; index++ ) {
          if ( ! isNullArray[index] ) {
            columnArray[index] = valueArray[i];
            n++;
          }
        }
      }

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new OptimizedNullArrayCellManager(
          columnBinary.columnType , startIndex , columnArray ) );

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
      return column;
    }

    @Override
    public List<String> getColumnKeys() {
      return new ArrayList<String>();
    }

    @Override
    public int getColumnSize() {
      return 0;
    }

  }

}
