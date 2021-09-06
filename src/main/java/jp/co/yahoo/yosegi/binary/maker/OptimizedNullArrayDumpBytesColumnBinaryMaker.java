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
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.StringRangeBlockIndex;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.Utf8BytesLinkObj;
import jp.co.yahoo.yosegi.spread.analyzer.BytesColumnAnalizeResult;
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
import java.util.List;

public class OptimizedNullArrayDumpBytesColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, minLength , maxLength , nullLength, lengthByteLength
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 5;

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

    byte[][] objList = new byte[column.size()][];
    int totalLength = 0;
    int logicalDataLength = 0;
    boolean[] isNullArray = new boolean[column.size()];
    DetermineMinMax<Integer> lengthMinMax = DetermineMinMaxFactory.createInt();
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

    for ( int i = startIndex,nullIndex = 0 ; i < column.size() ; i++,nullIndex++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        nullMaxIndex = nullIndex;
        isNullArray[nullIndex] = true;
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      byte[] obj = byteCell.getRow().getBytes();
      if ( obj == null ) {
        nullCount++;
        nullMaxIndex = nullIndex;
        isNullArray[nullIndex] = true;
        continue;
      }
      objList[rowCount] = obj;

      lengthMinMax.set( obj.length );

      totalLength += obj.length;
      logicalDataLength += Integer.BYTES + obj.length;

      notNullMaxIndex = nullIndex;
      rowCount++;
    }

    int nullLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    int lengthByteLength = 0;
    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter( lengthMinMax.getMin() , lengthMinMax.getMax() );
    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      lengthByteLength = lengthConverter.calcBinarySize( rowCount );
    }

    ByteOrder order = ByteOrder.nativeOrder();
    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

    int binaryLength = 
        META_LENGTH
        + nullLength
        + lengthByteLength
        + totalLength;
    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , binaryRaw.length );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( lengthMinMax.getMin() );
    wrapBuffer.putInt( lengthMinMax.getMax() );
    wrapBuffer.putInt( nullLength );
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

    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      IWriteSupporter lengthWriter = lengthConverter.toWriteSuppoter(
          rowCount , binaryRaw , META_LENGTH + nullLength , lengthByteLength  );
      for ( int i = 0 ; i < rowCount; i++ ) {
        lengthWriter.putInt( objList[i].length );
      }
    }

    ByteBuffer valueBuffer = ByteBuffer.wrap(
        binaryRaw , META_LENGTH + nullLength + lengthByteLength , totalLength );
    for ( int i = 0 ; i < rowCount ; i++ ) {
      valueBuffer.put( objList[i] );
    }
    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] binary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.BYTES ,
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
    BytesColumnAnalizeResult bytesAnalizeResult = (BytesColumnAnalizeResult)analizeResult;
    int startIndex = analizeResult.getRowStart();
    int maxIndex = analizeResult.getRowEnd();
    int nullCount = analizeResult.getNullCount() - startIndex;
    int notNullCount = analizeResult.getRowCount();

    int nullIndexLength =
        NullBinaryEncoder.getBinarySize( nullCount , notNullCount , maxIndex , maxIndex );
    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , analizeResult.getUniqCount() );

    int minLength = bytesAnalizeResult.getMinBytes();
    int maxLength = bytesAnalizeResult.getMaxBytes();
    int lengthBinaryLength = 0;

    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter( minLength , maxLength );
    if ( ! ( minLength == maxLength ) ) {
      lengthBinaryLength = lengthConverter.calcBinarySize( notNullCount );
    }
    return META_LENGTH
        + nullIndexLength
        + lengthBinaryLength 
        + bytesAnalizeResult.getLogicalDataSize();
  }

  @Override
  public IColumn toColumn(final ColumnBinary columnBinary) throws IOException {
    int loadCount =
        (columnBinary.loadIndex == null) ? columnBinary.rowCount : columnBinary.loadIndex.length;
    return new YosegiLoaderFactory().create(columnBinary, loadCount);
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    if (columnBinary.loadIndex == null) {
      return LoadType.SEQUENTIAL;
    } else {
      return LoadType.DICTIONARY;
    }
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);
    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullLength);

    IReadSupporter lengthReader;
    if (minLength == maxLength) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter(minLength);
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter(minLength, maxLength);
      lengthReader =
          lengthConverter.toReadSupporter(binary, META_LENGTH + nullLength, lengthBinaryLength);
    }

    int currentStart = META_LENGTH + nullLength + lengthBinaryLength;
    int index = 0;
    for (; index < startIndex; index++) {
      loader.setNull(index);
    }
    for (int i = 0; i < isNullArray.length; i++, index++) {
      if (isNullArray[i]) {
        loader.setNull(index);
      } else {
        int currentLength = lengthReader.getInt();
        loader.setBytes(index, binary, currentStart, currentLength);
        currentStart += currentLength;
      }
    }
    // NOTE: null padding up to load size.
    for (int i = index; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final IDictionaryLoader loader) throws IOException {
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);
    ByteOrder order = wrapBuffer.get() == (byte) 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullLength);

    IReadSupporter lengthReader;
    if (minLength == maxLength) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter(minLength);
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter(minLength, maxLength);
      lengthReader =
          lengthConverter.toReadSupporter(binary, META_LENGTH + nullLength, lengthBinaryLength);
    }

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int previousLoadIndex = -1;
    int lastIndex = startIndex + isNullArray.length - 1;
    for (int loadIndex : columnBinary.loadIndex) {
      if (loadIndex < 0) {
        throw new IOException("Index must be equal to or greater than 0.");
      } else if (loadIndex < previousLoadIndex) {
        throw new IOException("Index must be equal to or greater than the previous number.");
      }
      if (loadIndex > lastIndex) {
        break;
      }
      if (loadIndex >= startIndex && !isNullArray[loadIndex - startIndex]) {
        if (previousLoadIndex != loadIndex) {
          dictionarySize++;
        }
      }
      previousLoadIndex = loadIndex;
    }
    loader.createDictionary(dictionarySize);

    // NOTE:
    //   Set value to dict: dictionaryIndex, value
    //   Set dictionaryIndex: loadIndexArrayOffset, dictionaryIndex
    previousLoadIndex = -1; // NOTE: reset
    int loadIndexArrayOffset = 0;
    int dictionaryIndex = -1;
    int readOffset = startIndex;
    int nextStart = META_LENGTH + nullLength + lengthBinaryLength;
    int currentStart = 0;
    int currentLength = 0;
    for (int loadIndex : columnBinary.loadIndex) {
      if (loadIndex > lastIndex) {
        break;
      }
      // NOTE: read skip
      for (; readOffset <= loadIndex; readOffset++) {
        if (readOffset >= startIndex && !isNullArray[readOffset - startIndex]) {
          currentLength = lengthReader.getInt();
          currentStart = nextStart;
          nextStart += currentLength;
        }
      }
      if (loadIndex < startIndex || isNullArray[loadIndex - startIndex]) {
        loader.setNull(loadIndexArrayOffset);
      } else {
        if (previousLoadIndex != loadIndex) {
          dictionaryIndex++;
          loader.setBytesToDic(dictionaryIndex, binary, currentStart, currentLength);
        }
        loader.setDictionaryIndex(loadIndexArrayOffset, dictionaryIndex);
      }
      previousLoadIndex = loadIndex;
      loadIndexArrayOffset++;
    }

    // NOTE: null padding up to load size.
    for (int i = loadIndexArrayOffset; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  @Override
  public void load(final ColumnBinary columnBinary, final ILoader loader) throws IOException {
    if (columnBinary.loadIndex == null) {
      if (loader.getLoaderType() != LoadType.SEQUENTIAL) {
        throw new IOException("Loader type is not SEQUENTIAL.");
      }
      loadFromColumnBinary(columnBinary, (ISequentialLoader) loader);
    } else {
      if (loader.getLoaderType() != LoadType.DICTIONARY) {
        throw new IOException("Loader type is not DICTIONARY.");
      }
      loadFromExpandColumnBinary(columnBinary, (IDictionaryLoader) loader);
    }
    loader.finish();
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress(
        columnBinary.binary ,
        columnBinary.binaryStart ,
        columnBinary.binaryLength );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

    allocator.setValueCount( startIndex + isNullArray.length );

    IReadSupporter lengthReader;
    if ( minLength == maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( minLength , maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + nullLength ,
          lengthBinaryLength );
    }

    int currentStart = META_LENGTH + nullLength + lengthBinaryLength;
    for ( int i = 0 ; i < startIndex ; i++ ) {
      allocator.setNull( i );
    }
    for ( int i = 0 ; i < isNullArray.length ; i++ ) {
      if ( isNullArray[i]  ) {
        allocator.setNull( i + startIndex );
      } else {
        int currentLength = lengthReader.getInt();
        allocator.setBytes( i + startIndex , binary , currentStart , currentLength );
        currentStart += currentLength;
      }
    }
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class BytesColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private PrimitiveColumn column;
    private boolean isCreate;

    public BytesColumnManager(
        final ColumnBinary columnBinary ) throws IOException {
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }
      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress( columnBinary.binary ,
          columnBinary.binaryStart ,
          columnBinary.binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
      ByteOrder order =
          wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      final int startIndex = wrapBuffer.getInt();
      int minLength = wrapBuffer.getInt();
      int maxLength = wrapBuffer.getInt();
      int nullLength = wrapBuffer.getInt();
      int lengthBinaryLength = wrapBuffer.getInt();

      boolean[] isNullArray =
          NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

      IReadSupporter lengthReader;
      if ( minLength == maxLength ) {
        lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
      } else {
        NumberToBinaryUtils.IIntConverter lengthConverter =
            NumberToBinaryUtils.getIntConverter( minLength , maxLength );
        lengthReader = lengthConverter.toReadSupporter(
            binary ,
            META_LENGTH + nullLength ,
            lengthBinaryLength );
      }

      PrimitiveObject[] valueArray = new PrimitiveObject[ isNullArray.length ];
      int currentStart = META_LENGTH + nullLength + lengthBinaryLength;
      for ( int i = 0 ; i < isNullArray.length ; i++ ) {
        if ( ! isNullArray[i]  ) {
          int currentLength = lengthReader.getInt();
          valueArray[i] = new Utf8BytesLinkObj( binary , currentStart , currentLength );
          currentStart += currentLength;
        }
      }
      column = new PrimitiveColumn( columnBinary.columnType , columnBinary.columnName );
      column.setCellManager( new OptimizedNullArrayCellManager(
          columnBinary.columnType , startIndex , valueArray ) );

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
