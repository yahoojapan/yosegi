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
import jp.co.yahoo.yosegi.binary.maker.index.RangeDoubleIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.DoubleRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.DoubleColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
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
import jp.co.yahoo.yosegi.util.io.nullencoder.NullBinaryEncoder;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimizedNullArrayDoubleColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, rowCount, nullIndexLength, indexLength
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 4;

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
    Map<Double,Integer> dicMap = new HashMap<Double,Integer>();
    double[] dicArray = new double[column.size()];
    int[] indexArray = new int[column.size()];
    boolean[] isNullArray = new boolean[column.size()];

    DetermineMinMax<Double> detemineMinMax = DetermineMinMaxFactory.createDouble();
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

    for ( int i = startIndex,arrayIndex = 0 ; i < column.size() ; i++ ,arrayIndex++) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        nullMaxIndex = arrayIndex;
        isNullArray[arrayIndex] = true;
        continue;
      }
      notNullMaxIndex = arrayIndex;
      PrimitiveCell primitiveCell = (PrimitiveCell) cell;
      PrimitiveObject primitiveObj = primitiveCell.getRow();
      Double target = Double.valueOf( primitiveObj.getDouble() );

      if ( ! dicMap.containsKey( target ) ) {
        detemineMinMax.set( target );
        int dicIndex = dicMap.size();
        dicMap.put( target , dicIndex );
        dicArray[dicIndex] = target.doubleValue();
      }
      indexArray[rowCount] = dicMap.get( target );
      rowCount++;
    }

    if ( nullCount == 0
        && detemineMinMax.getMin().equals( detemineMinMax.getMax() )
        && startIndex == 0 ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new DoubleObj( detemineMinMax.getMin() ) , column.getColumnName() , column.size() );
    }

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicMap.size() );

    int indexLength = indexConverter.calcBinarySize( rowCount );
    int dicLength = Double.BYTES * dicMap.size();

    ByteOrder order = ByteOrder.nativeOrder();

    int nullIndexLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    byte[] binaryRaw = new byte[ META_LENGTH + nullIndexLength + indexLength + dicLength ];

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.put( order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1 );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( rowCount );
    wrapBuffer.putInt( nullIndexLength );
    wrapBuffer.putInt( indexLength );
    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH ,
        nullIndexLength ,
        isNullArray ,
        nullCount ,
        rowCount ,
        nullMaxIndex ,
        notNullMaxIndex );
    IWriteSupporter indexWriter = indexConverter.toWriteSuppoter(
        rowCount , binaryRaw , META_LENGTH + nullIndexLength , indexLength  );
    for ( int i = 0 ; i < rowCount ; i++ ) {
      indexWriter.putInt( indexArray[i] );
    }
    IWriteSupporter dicWriter = ByteBufferSupporterFactory.createWriteSupporter(
        binaryRaw ,
        META_LENGTH + nullIndexLength + indexLength ,
        indexLength,
        order );
    for ( int i = 0 ; i < dicMap.size() ; i++ ) {
      dicWriter.putDouble( dicArray[i] );
    }

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    byte[] binary = new byte[ Double.BYTES * 2 + compressBinary.length ];

    wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    wrapBuffer.putDouble( detemineMinMax.getMin() );
    wrapBuffer.putDouble( detemineMinMax.getMax() );
    wrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binaryRaw.length ,
        Double.BYTES * rowCount ,
        dicMap.size() ,
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

    int nullIndexLength =
        NullBinaryEncoder.getBinarySize( nullCount , notNullCount , maxIndex , maxIndex );
    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , analizeResult.getUniqCount() );

    int indexLength = indexConverter.calcBinarySize( notNullCount );
    int dicLength = Double.BYTES * analizeResult.getUniqCount();

    return META_LENGTH + nullIndexLength + indexLength + dicLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );

    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new ColumnManager(
        columnBinary
      ) ,
      new RangeDoubleIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    int start = columnBinary.binaryStart + ( Double.BYTES * 2 );
    int length = columnBinary.binaryLength - ( Double.BYTES * 2 );

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress( columnBinary.binary , start , length );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );

    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowCount = wrapBuffer.getInt();
    int nullIndexLength = wrapBuffer.getInt();
    int indexLength = wrapBuffer.getInt();
    int dicLength = binary.length - META_LENGTH - nullIndexLength - indexLength;
    int dicSize = dicLength / Double.BYTES;

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicSize );

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullIndexLength );

    IReadSupporter dicReader = ByteBufferSupporterFactory.createReadSupporter(
        binary,
        META_LENGTH + nullIndexLength + indexLength,
        dicLength,
        order );
    double[] dicArray = new double[dicSize];
    for ( int i = 0 ; i < dicArray.length ; i++ ) {
      dicArray[i] = dicReader.getDouble();
    }

    allocator.setValueCount( startIndex + isNullArray.length );

    IReadSupporter indexReader =
        indexConverter.toReadSupporter( binary , META_LENGTH + nullIndexLength , indexLength );
    int index = startIndex;
    for ( ; index < startIndex ; index++ ) {
      allocator.setNull( index );
    }
    for ( int i = 0 ; i < isNullArray.length ; i++,index++ ) {
      if ( isNullArray[i]  ) {
        allocator.setNull( index );
      } else {
        allocator.setDouble( index , dicArray[indexReader.getInt()] );
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
    Double min = Double.valueOf( wrapBuffer.getDouble() );
    Double max = Double.valueOf( wrapBuffer.getDouble() );
    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new DoubleRangeBlockIndex( min , max ) );
  }

  public class ColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;

    private PrimitiveColumn column;
    private boolean isCreate;

    public ColumnManager(
        final ColumnBinary columnBinary ) {
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      int start = columnBinary.binaryStart + ( Double.BYTES * 2 );
      int length = columnBinary.binaryLength - ( Double.BYTES * 2 );

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , start , length );

      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );

      ByteOrder order = wrapBuffer.get() == (byte)0
          ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      final int startIndex = wrapBuffer.getInt();
      int rowCount = wrapBuffer.getInt();
      int nullIndexLength = wrapBuffer.getInt();
      int indexLength = wrapBuffer.getInt();
      int dicLength = binary.length - META_LENGTH - nullIndexLength - indexLength;
      int dicSize = dicLength / Double.BYTES;

      NumberToBinaryUtils.IIntConverter indexConverter =
          NumberToBinaryUtils.getIntConverter( 0 , dicSize );

      boolean[] isNullArray =
          NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullIndexLength ); 

      IReadSupporter indexReader =
          indexConverter.toReadSupporter( binary , META_LENGTH + nullIndexLength , indexLength );
      int[] indexArray = new int[isNullArray.length];
      for ( int i = 0 ; i < indexArray.length ; i++ ) {
        if ( ! isNullArray[i] ) {
          indexArray[i] = indexReader.getInt();
        }
      }

      IReadSupporter dicReader = ByteBufferSupporterFactory.createReadSupporter(
          binary,
          META_LENGTH + nullIndexLength + indexLength,
          dicLength,
          order );
      PrimitiveObject[] dicArray = new PrimitiveObject[dicSize];
      for ( int i = 0 ; i < dicArray.length ; i++ ) {
        dicArray[i] = new DoubleObj( dicReader.getDouble() );
      }

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new OptimizedNullArrayDicCellManager(
          columnBinary.columnType , startIndex , isNullArray , indexArray , dicArray ) );

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
