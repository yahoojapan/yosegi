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
import jp.co.yahoo.yosegi.binary.maker.index.RangeStringIndex;
import jp.co.yahoo.yosegi.binary.maker.index.SequentialStringCellIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.Utf8BytesLinkObj;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.StringColumnAnalizeResult;
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

public class DictionaryRleStringColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, rowGroupCount , maxRowGroupCount , dicSize,
  //   minLength,maxLength,nullLength,rowGroupIndexLength,rowGroupBinaryLength,lengthByteLength
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 10;

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

    int startIndex = 0;
    for ( ; startIndex < column.size() ; startIndex++ ) {
      ICell cell = column.get(startIndex);
      if ( cell.getType() != ColumnType.NULL ) {
        break;
      }
    }

    int logicalDataLength = 0;
    boolean[] isNullArray = new boolean[column.size()];
    DetermineMinMax<String> detemineMinMax = DetermineMinMaxFactory.createString();
    DetermineMinMax<Integer> lengthMinMax = DetermineMinMaxFactory.createInt();
    int rowCount = 0;
    int nullCount = 0;
    int nullMaxIndex = 0;
    int notNullMaxIndex = 0;

    Map<String,Integer> dicMap = new HashMap<String,Integer>();
    byte[][] objList = new byte[column.size()][];
    int totalLength = 0;
    int[] rowGroupLengthArray = new int[column.size()];
    int[] rowGroupIndexArray = new int[column.size()];
    int rowGroupCount = 0;
    int maxRowGroupLength = 0;
    String currentValue = null;
    byte[] currentRawValue = null;
    int currentRowGroupLength = 0;

    for ( int i = startIndex,nullIndex = 0 ; i < column.size() ; i++,nullIndex++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        nullMaxIndex = nullIndex;
        isNullArray[nullIndex] = true;
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      String strObj = byteCell.getRow().getString();
      if ( strObj == null ) {
        nullCount++;
        nullMaxIndex = nullIndex;
        isNullArray[nullIndex] = true;
        continue;
      }
      if ( currentValue == null ) {
        currentValue = strObj;
        currentRawValue = byteCell.getRow().getBytes();
      }
      if ( ! currentValue.equals( strObj ) ) {
        if ( ! dicMap.containsKey( currentValue ) ) {
          int index = dicMap.size();
          dicMap.put( currentValue , index );
          objList[index] = currentRawValue;
          totalLength += currentRawValue.length;
          lengthMinMax.set( currentRawValue.length );
          detemineMinMax.set( currentValue );
        }
        int index = dicMap.get( currentValue );
        rowGroupIndexArray[rowGroupCount] = index;
        rowGroupLengthArray[rowGroupCount] = currentRowGroupLength;
        rowGroupCount++;
        if ( maxRowGroupLength < currentRowGroupLength ) {
          maxRowGroupLength = currentRowGroupLength;
        }

        currentValue = strObj;
        currentRawValue = byteCell.getRow().getBytes();
        currentRowGroupLength = 0;
      }
      currentRowGroupLength++;

      logicalDataLength += Integer.BYTES + currentRawValue.length;
      notNullMaxIndex = nullIndex;
      rowCount++;
    }
    if ( ! dicMap.containsKey( currentValue ) ) {
      int index = dicMap.size();
      dicMap.put( currentValue , index );
      objList[index] = currentRawValue;
      totalLength += currentRawValue.length;
      lengthMinMax.set( currentRawValue.length );
      detemineMinMax.set( currentValue );
    }
    int index = dicMap.get( currentValue );
    rowGroupIndexArray[rowGroupCount] = index;
    rowGroupLengthArray[rowGroupCount] = currentRowGroupLength;
    rowGroupCount++;
    if ( maxRowGroupLength < currentRowGroupLength ) {
      maxRowGroupLength = currentRowGroupLength;
    }

    int nullLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    NumberToBinaryUtils.IIntConverter rowGroupIndexEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , dicMap.size() );
    int rowGroupIndexLength = rowGroupIndexEncoder.calcBinarySize( rowGroupCount );

    NumberToBinaryUtils.IIntConverter rowGroupLengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupLength );
    int rowGroupBinaryLength = rowGroupLengthEncoder.calcBinarySize( rowGroupCount );

    int lengthByteLength = 0;
    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter( lengthMinMax.getMin() , lengthMinMax.getMax() );
    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      lengthByteLength = lengthConverter.calcBinarySize( dicMap.size() );
    }

    ByteOrder order = ByteOrder.nativeOrder();
    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

    int binaryLength = 
        META_LENGTH
        + nullLength
        + rowGroupIndexLength
        + rowGroupBinaryLength
        + lengthByteLength
        + totalLength;
    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , binaryRaw.length );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( rowGroupCount );
    wrapBuffer.putInt( maxRowGroupLength );
    wrapBuffer.putInt( dicMap.size() );
    wrapBuffer.putInt( lengthMinMax.getMin() );
    wrapBuffer.putInt( lengthMinMax.getMax() );
    wrapBuffer.putInt( nullLength );
    wrapBuffer.putInt( rowGroupIndexLength );
    wrapBuffer.putInt( rowGroupBinaryLength );
    wrapBuffer.putInt( lengthByteLength );

    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH ,
        nullLength ,
        isNullArray ,
        nullCount ,
        rowCount ,
        nullMaxIndex ,
        notNullMaxIndex );

    IWriteSupporter rowGroupIndexWriter = rowGroupLengthEncoder.toWriteSuppoter(
        rowGroupCount ,
        binaryRaw , META_LENGTH + nullLength ,
        rowGroupIndexLength  );
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      rowGroupIndexWriter.putInt( rowGroupIndexArray[i] );
    }

    IWriteSupporter rowGroupLengthWriter = rowGroupLengthEncoder.toWriteSuppoter(
        rowGroupCount ,
        binaryRaw , META_LENGTH + nullLength + rowGroupIndexLength,
        rowGroupBinaryLength  );
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      rowGroupLengthWriter.putInt( rowGroupLengthArray[i] );
    }

    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      IWriteSupporter lengthWriter = lengthConverter.toWriteSuppoter(
          dicMap.size() ,
          binaryRaw ,
          META_LENGTH + nullLength + rowGroupIndexLength + rowGroupBinaryLength ,
          lengthByteLength  );
      for ( int i = 0 ; i < dicMap.size(); i++ ) {
        lengthWriter.putInt( objList[i].length );
      }
    }

    ByteBuffer valueBuffer = ByteBuffer.wrap(
        binaryRaw ,
        META_LENGTH + nullLength + rowGroupIndexLength + rowGroupBinaryLength + lengthByteLength ,
        totalLength );
    for ( int i = 0 ; i < dicMap.size() ; i++ ) {
      valueBuffer.put( objList[i] );
    }
    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinaryRaw = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    int minCharLength = Character.BYTES * detemineMinMax.getMin().length();
    int maxCharLength = Character.BYTES * detemineMinMax.getMax().length();
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    byte[] binary = new byte[headerSize + compressBinaryRaw.length];
    ByteBuffer binaryWrapBuffer = ByteBuffer.wrap( binary );
    binaryWrapBuffer.putInt( minCharLength );
    binaryWrapBuffer.asCharBuffer().put( detemineMinMax.getMin() );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + minCharLength );
    binaryWrapBuffer.putInt( maxCharLength );
    binaryWrapBuffer.asCharBuffer().put( detemineMinMax.getMax() );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + maxCharLength );
    binaryWrapBuffer.put( compressBinaryRaw );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.STRING ,
        column.size() ,
        binaryRaw.length ,
        logicalDataLength ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    StringColumnAnalizeResult stringAnalizeResult = (StringColumnAnalizeResult)analizeResult;
    int startIndex = analizeResult.getRowStart();
    int maxIndex = analizeResult.getRowEnd();
    int nullCount = analizeResult.getNullCount() - startIndex;
    int notNullCount = analizeResult.getRowCount();
    int nullIgnoreRleRowGroupCount = analizeResult.getNullIgnoreRleGroupCount();
    int nullIgnoreRleMaxRowGroupLength = analizeResult.getNullIgonoreRleMaxRowGroupLength();
    int nullIgnoreRleTotalLength = stringAnalizeResult.getNullIgnoreRleTotalUtf8Bytes();

    int nullIndexLength =
        NullBinaryEncoder.getBinarySize( nullCount , notNullCount , maxIndex , maxIndex );

    int minLength = stringAnalizeResult.getMinUtf8Bytes();
    int maxLength = stringAnalizeResult.getMaxUtf8Bytes();
    int lengthBinaryLength = 0;

    NumberToBinaryUtils.IIntConverter rowGroupIndexEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , analizeResult.getUniqCount() );
    int rowGroupIndexLength = rowGroupIndexEncoder.calcBinarySize( nullIgnoreRleRowGroupCount );

    NumberToBinaryUtils.IIntConverter rowGroupLengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , nullIgnoreRleMaxRowGroupLength );
    int rowGroupBinaryLength = rowGroupLengthEncoder.calcBinarySize( nullIgnoreRleRowGroupCount );

    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter( minLength , maxLength );
    if ( ! ( minLength == maxLength ) ) {
      lengthBinaryLength = lengthConverter.calcBinarySize( analizeResult.getUniqCount() );
    }
    int dicLength = stringAnalizeResult.getUniqUtf8ByteSize();
    return META_LENGTH
        + nullIndexLength
        + rowGroupIndexLength
        + rowGroupBinaryLength
        + lengthBinaryLength 
        + dicLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    int headerSize = Integer.BYTES + minLength + Integer.BYTES + maxLength;
    return new HeaderIndexLazyColumn(
      columnBinary.columnName ,
      columnBinary.columnType ,
      new StringColumnManager(
        columnBinary ,
        columnBinary.binaryStart + headerSize ,
        columnBinary.binaryLength - headerSize ) ,
      new RangeStringIndex( min , max )
    );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ByteBuffer headerWrapBuffer = ByteBuffer.wrap(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    int minCharLength = headerWrapBuffer.getInt();
    headerWrapBuffer.position( headerWrapBuffer.position() + minCharLength );

    int maxCharLength = headerWrapBuffer.getInt();
    headerWrapBuffer.position( headerWrapBuffer.position() + maxCharLength );
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress(
        columnBinary.binary ,
        columnBinary.binaryStart + headerSize ,
        columnBinary.binaryLength - headerSize );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowGroupCount = wrapBuffer.getInt();
    int maxRowGroupCount = wrapBuffer.getInt();
    int dicSize = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int rowGroupIndexLength = wrapBuffer.getInt();
    int rowGroupBinaryLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

    allocator.setValueCount( startIndex + isNullArray.length );

    NumberToBinaryUtils.IIntConverter rowGroupIndexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicSize );
    IReadSupporter rowGroupIndexReader = rowGroupIndexConverter.toReadSupporter(
        binary,
        META_LENGTH + nullLength ,
        rowGroupIndexLength );

    NumberToBinaryUtils.IIntConverter rowGroupLengthConverter =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
    IReadSupporter rowGroupLengthReader = rowGroupLengthConverter.toReadSupporter(
        binary,
        META_LENGTH + nullLength + rowGroupIndexLength,
        rowGroupBinaryLength );

    IReadSupporter lengthReader;
    if ( minLength == maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( minLength , maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + nullLength + rowGroupIndexLength + rowGroupBinaryLength ,
          lengthBinaryLength );
    }
    Utf8BytesLinkObj[] dicArray = new Utf8BytesLinkObj[ dicSize ];
    int currentStart = META_LENGTH 
        + nullLength 
        + rowGroupIndexLength
        + rowGroupBinaryLength 
        + lengthBinaryLength;
    for ( int i = 0 ; i < dicArray.length ; i++ ) {
      int currentLength = lengthReader.getInt();
      dicArray[i] = new Utf8BytesLinkObj( binary , currentStart , currentLength );
      currentStart += currentLength;
    }

    for ( int i = 0 ; i < startIndex ; i++ ) {
      allocator.setNull( i );
    }
    int index = 0;
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      Utf8BytesLinkObj obj = dicArray[ rowGroupIndexReader.getInt() ];
      int rowGroupLength = rowGroupLengthReader.getInt();
      for ( int n = 0 ; n < rowGroupLength ; index++ ) {
        if ( isNullArray[index] ) {
          allocator.setNull( index + startIndex );
          continue;
        }
        allocator.setBytes(
            index + startIndex ,
            dicArray[index].getLinkBytes() ,
            dicArray[index].getStart() ,
            dicArray[index].getLength() );
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
    int minLength = wrapBuffer.getInt();
    char[] minCharArray = new char[minLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( minCharArray );
    wrapBuffer.position( wrapBuffer.position() + minLength );

    int maxLength = wrapBuffer.getInt();
    char[] maxCharArray = new char[maxLength / Character.BYTES];
    wrapBuffer.asCharBuffer().get( maxCharArray );
    wrapBuffer.position( wrapBuffer.position() + maxLength );

    String min = new String( minCharArray );
    String max = new String( maxCharArray );

    BlockIndexNode currentNode = parentNode.getChildNode( columnBinary.columnName );
    currentNode.setBlockIndex( new StringRangeBlockIndex( min , max ) );
  }

  public class StringColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private final int binaryStart;
    private final int binaryLength;
    private PrimitiveColumn column;
    private boolean isCreate;

    /**
     * Init.
     */
    public StringColumnManager(
        final ColumnBinary columnBinary ,
        final int binaryStart ,
        final int binaryLength ) throws IOException {
      this.columnBinary = columnBinary;
      this.binaryStart = binaryStart;
      this.binaryLength = binaryLength;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary , binaryStart , binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
      ByteOrder order =
          wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      final int startIndex = wrapBuffer.getInt();
      final int rowGroupCount = wrapBuffer.getInt();
      int maxRowGroupCount = wrapBuffer.getInt();
      int dicSize = wrapBuffer.getInt();
      int minLength = wrapBuffer.getInt();
      int maxLength = wrapBuffer.getInt();
      int nullLength = wrapBuffer.getInt();
      int rowGroupIndexLength = wrapBuffer.getInt();
      int rowGroupBinaryLength = wrapBuffer.getInt();
      int lengthBinaryLength = wrapBuffer.getInt();

      boolean[] isNullArray =
          NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

      NumberToBinaryUtils.IIntConverter rowGroupIndexConverter =
          NumberToBinaryUtils.getIntConverter( 0 , dicSize );
      IReadSupporter rowGroupIndexReader = rowGroupIndexConverter.toReadSupporter(
          binary,
          META_LENGTH + nullLength ,
          rowGroupIndexLength );

      NumberToBinaryUtils.IIntConverter rowGroupLengthConverter =
          NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
      IReadSupporter rowGroupLengthReader = rowGroupLengthConverter.toReadSupporter(
          binary,
          META_LENGTH + nullLength + rowGroupIndexLength,
          rowGroupBinaryLength );

      IReadSupporter lengthReader;
      if ( minLength == maxLength ) {
        lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
      } else {
        NumberToBinaryUtils.IIntConverter lengthConverter =
            NumberToBinaryUtils.getIntConverter( minLength , maxLength );
        lengthReader = lengthConverter.toReadSupporter(
            binary ,
            META_LENGTH + nullLength + rowGroupIndexLength + rowGroupBinaryLength ,
            lengthBinaryLength );
      }
      Utf8BytesLinkObj[] dicArray = new Utf8BytesLinkObj[ dicSize ];
      int currentStart = META_LENGTH
          + nullLength
          + rowGroupIndexLength
          + rowGroupBinaryLength
          + lengthBinaryLength;
      for ( int i = 0 ; i < dicArray.length ; i++ ) {
        int currentLength = lengthReader.getInt();
        dicArray[i] = new Utf8BytesLinkObj( binary , currentStart , currentLength );
        currentStart += currentLength;
      }

      int[] indexArray = new int[isNullArray.length];
      int index = 0;
      for ( int i = 0 ; i < rowGroupCount ; i++ ) {
        int rowGroupLength = rowGroupLengthReader.getInt();
        int valueIndex = rowGroupIndexReader.getInt();
        for ( int n = 0 ; n < rowGroupLength ; index++ ) {
          if ( ! isNullArray[index] ) {
            indexArray[index] = valueIndex;
            n++;
          }
        }
      }

      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new OptimizedNullArrayDicCellManager(
          columnBinary.columnType , startIndex , isNullArray , indexArray , dicArray ) );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      try {
        create();
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
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
