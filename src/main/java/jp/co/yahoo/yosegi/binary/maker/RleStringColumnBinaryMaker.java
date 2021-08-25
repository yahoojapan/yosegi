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
import jp.co.yahoo.yosegi.inmemory.IDictionary;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
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
import java.util.List;

public class RleStringColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, rowGroupCount , maxRowGroupCount ,
  //   minLength , maxLength , nullLength, rowGroupBinaryLength, lengthByteLength
  private static final int META_LENGTH = Byte.BYTES + Integer.BYTES * 8;

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

    byte[][] objList = new byte[column.size()][];
    int totalLength = 0;
    int[] rowGroupLengthArray = new int[column.size()];
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
        objList[rowGroupCount] = currentRawValue;
        rowGroupLengthArray[rowGroupCount] = currentRowGroupLength;
        rowGroupCount++;
        if ( maxRowGroupLength < currentRowGroupLength ) {
          maxRowGroupLength = currentRowGroupLength;
        }
        lengthMinMax.set( currentRawValue.length );
        detemineMinMax.set( currentValue );
        totalLength += currentRawValue.length;

        currentValue = strObj;
        currentRawValue = byteCell.getRow().getBytes();
        currentRowGroupLength = 0;
      }
      currentRowGroupLength++;

      logicalDataLength += Integer.BYTES + currentRawValue.length;
      notNullMaxIndex = nullIndex;
      rowCount++;
    }
    objList[rowGroupCount] = currentRawValue;
    rowGroupLengthArray[rowGroupCount] = currentRowGroupLength;
    rowGroupCount++;
    if ( maxRowGroupLength < currentRowGroupLength ) {
      maxRowGroupLength = currentRowGroupLength;
    }
    lengthMinMax.set( currentRawValue.length );
    detemineMinMax.set( currentValue );
    totalLength += currentRawValue.length;

    int nullLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    NumberToBinaryUtils.IIntConverter rowGroupLengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupLength );
    int rowGroupBinaryLength = rowGroupLengthEncoder.calcBinarySize( rowGroupCount );

    int lengthByteLength = 0;
    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter(
            lengthMinMax.getMin() , lengthMinMax.getMax() );
    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      lengthByteLength = lengthConverter.calcBinarySize( rowGroupCount );
    }

    ByteOrder order = ByteOrder.nativeOrder();
    byte byteOrderByte = order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1;

    int binaryLength = 
        META_LENGTH
        + nullLength
        + rowGroupBinaryLength
        + lengthByteLength
        + totalLength;
    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , binaryRaw.length );
    wrapBuffer.put( byteOrderByte );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( rowGroupCount );
    wrapBuffer.putInt( maxRowGroupLength );
    wrapBuffer.putInt( lengthMinMax.getMin() );
    wrapBuffer.putInt( lengthMinMax.getMax() );
    wrapBuffer.putInt( nullLength );
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

    IWriteSupporter rowGroupLengthWriter = rowGroupLengthEncoder.toWriteSuppoter(
        rowGroupCount ,
        binaryRaw , META_LENGTH + nullLength ,
        rowGroupBinaryLength  );
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      rowGroupLengthWriter.putInt( rowGroupLengthArray[i] );
    }

    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      IWriteSupporter lengthWriter = lengthConverter.toWriteSuppoter(
          rowGroupCount ,
          binaryRaw ,
          META_LENGTH + nullLength + rowGroupBinaryLength ,
          lengthByteLength  );
      for ( int i = 0 ; i < rowGroupCount; i++ ) {
        lengthWriter.putInt( objList[i].length );
      }
    }

    ByteBuffer valueBuffer = ByteBuffer.wrap(
        binaryRaw ,
        META_LENGTH + nullLength + rowGroupBinaryLength + lengthByteLength ,
        totalLength );
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
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

    NumberToBinaryUtils.IIntConverter rowGroupLengthEncoder =
        NumberToBinaryUtils.getIntConverter( 0 , nullIgnoreRleMaxRowGroupLength );
    int rowGroupBinaryLength = rowGroupLengthEncoder.calcBinarySize( nullIgnoreRleRowGroupCount );

    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter( minLength , maxLength );
    if ( ! ( minLength == maxLength ) ) {
      lengthBinaryLength = lengthConverter.calcBinarySize( nullIgnoreRleRowGroupCount );
    }
    return META_LENGTH
        + nullIndexLength
        + rowGroupBinaryLength
        + lengthBinaryLength 
        + nullIgnoreRleTotalLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    int loadCount = columnBinary.rowCount;
    if ( columnBinary.loadIndex != null ) {
      loadCount = columnBinary.loadIndex.length;
    }
    ILoader<IColumn> loader = new YosegiLoaderFactory().createLoader(
        columnBinary , loadCount , getLoadType( columnBinary ) );
    load( columnBinary , loader );
    return loader.build();
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary ) {
    return LoadType.DICTIONARY;
  }

  private byte[] getDecompressBinary( final ColumnBinary columnBinary ) throws IOException {
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
    return binary;
  }

  private void loadFromColumnBinary(
      final ColumnBinary columnBinary , final IDictionaryLoader loader ) throws IOException {
    byte[] binary = getDecompressBinary( columnBinary );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowGroupCount = wrapBuffer.getInt();
    int maxRowGroupCount = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int rowGroupBinaryLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

    NumberToBinaryUtils.IIntConverter rowGroupLengthConverter =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
    IReadSupporter rowGroupLengthReader = rowGroupLengthConverter.toReadSupporter(
        binary,
        META_LENGTH + nullLength ,
        rowGroupBinaryLength );

    IReadSupporter lengthReader;
    if ( minLength == maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( minLength , maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + nullLength + rowGroupBinaryLength ,
          lengthBinaryLength );
    }
    int currentStart = META_LENGTH + nullLength + rowGroupBinaryLength + lengthBinaryLength;
    loader.createDictionary( rowGroupCount );
    for ( int i = 0 ; i < startIndex ; i++ ) {
      loader.setNull( i );
    }

    int index = 0;
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      int rowGroupLength = rowGroupLengthReader.getInt();
      int binaryLength = lengthReader.getInt();
      loader.setBytesToDic( i , binary , currentStart , binaryLength );
      currentStart += binaryLength;
      for ( int n = 0 ; n < rowGroupLength ; index++ ) {
        if ( isNullArray[index] ) {
          loader.setNull( index + startIndex );
          continue;
        }
        loader.setDictionaryIndex( index + startIndex , i );
        n++;
      }
    }

    for ( int i = isNullArray.length + startIndex ; i < loader.getLoadSize() ; i++ ) {
      loader.setNull( i );
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary , final IDictionaryLoader loader ) throws IOException {
    byte[] binary = getDecompressBinary( columnBinary );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowGroupCount = wrapBuffer.getInt();
    int maxRowGroupCount = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int rowGroupBinaryLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

    NumberToBinaryUtils.IIntConverter rowGroupLengthConverter =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
    IReadSupporter rowGroupLengthReader = rowGroupLengthConverter.toReadSupporter(
        binary,
        META_LENGTH + nullLength ,
        rowGroupBinaryLength );

    IReadSupporter lengthReader;
    if ( minLength == maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( minLength , maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + nullLength + rowGroupBinaryLength ,
          lengthBinaryLength );
    }
    int index = 0;
    int[] rowGroupDicIndexArray = new int[isNullArray.length];
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      int rowGroupLength = rowGroupLengthReader.getInt();
      for ( int n = 0 ; n < rowGroupLength ; index++ ) {
        if ( ! isNullArray[index] ) {
          rowGroupDicIndexArray[index] = i;
          n++;
        }
      }
    }

    int currentLoadIndex = 0;
    boolean[] isNeedDictionary = new boolean[rowGroupCount];
    int[] newDicIndexList = new int[rowGroupCount];
    int needDicCount = 0;
    for ( int loadIndex : columnBinary.loadIndex ) {
      if ( loadIndex < 0 ) {
        throw new IOException(
            "Index must be greater than 0." );
      } else if ( loadIndex < currentLoadIndex ) {
        throw new IOException( "Index must be equal to or greater than the previous number." );
      }
      if ( startIndex + isNullArray.length <= loadIndex ) {
        break;
      }
      currentLoadIndex = loadIndex;
      if ( loadIndex < startIndex || isNullArray[loadIndex - startIndex] ) {
        continue;
      }
      if ( ! isNeedDictionary[rowGroupDicIndexArray[loadIndex - startIndex]] ) {
        isNeedDictionary[rowGroupDicIndexArray[loadIndex - startIndex]] = true;
        newDicIndexList[rowGroupDicIndexArray[loadIndex - startIndex]] = needDicCount;
        needDicCount++;
      }
    }
    loader.createDictionary( needDicCount );

    int addDicCount = 0;
    int currentStart = META_LENGTH + nullLength + rowGroupBinaryLength + lengthBinaryLength;
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      int currentLength = lengthReader.getInt();
      if ( isNeedDictionary[i] ) {
        loader.setBytesToDic( addDicCount , binary , currentStart , currentLength );
        addDicCount++;
      }
      currentStart += currentLength;
    }

    int currentColumnIndex = 0;
    for ( int loadIndex : columnBinary.loadIndex ) {
      if ( startIndex + isNullArray.length <= loadIndex
          || loadIndex < startIndex
          || isNullArray[loadIndex - startIndex] ) {
        loader.setNull( currentColumnIndex );
      } else {
        loader.setDictionaryIndex(
            currentColumnIndex , newDicIndexList[rowGroupDicIndexArray[loadIndex - startIndex]] );
      }
      currentColumnIndex++;
    }
  }

  @Override
  public void load(
      final ColumnBinary columnBinary , final ILoader loader ) throws IOException {
    if ( loader.getLoaderType() != LoadType.DICTIONARY ) {
      throw new IOException( "Loader type is not DICTIONARY." );
    }
    if ( columnBinary.loadIndex == null ) {
      loadFromColumnBinary( columnBinary , (IDictionaryLoader)loader );
    } else {
      loadFromExpandColumnBinary( columnBinary , (IDictionaryLoader)loader );
    }
    loader.finish();
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    byte[] binary = getDecompressBinary( columnBinary );

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteOrder order = wrapBuffer.get() == (byte)0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowGroupCount = wrapBuffer.getInt();
    int maxRowGroupCount = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int rowGroupBinaryLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , nullLength );

    allocator.setValueCount( startIndex + isNullArray.length );

    NumberToBinaryUtils.IIntConverter rowGroupLengthConverter =
        NumberToBinaryUtils.getIntConverter( 0 , maxRowGroupCount );
    IReadSupporter rowGroupLengthReader = rowGroupLengthConverter.toReadSupporter(
        binary,
        META_LENGTH + nullLength ,
        rowGroupBinaryLength );

    IReadSupporter lengthReader;
    if ( minLength == maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( minLength , maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + nullLength + rowGroupBinaryLength ,
          lengthBinaryLength );
    }

    int currentStart = META_LENGTH + nullLength + rowGroupBinaryLength + lengthBinaryLength;
    IDictionary dic = allocator.createDictionary( rowGroupCount );
    for ( int i = 0 ; i < startIndex ; i++ ) {
      allocator.setNull( i );
    }
    int index = 0;
    for ( int i = 0 ; i < rowGroupCount ; i++ ) {
      int rowGroupLength = rowGroupLengthReader.getInt();
      int binaryLength = lengthReader.getInt();
      dic.setBytes( i , binary , currentStart , binaryLength );
      for ( int n = 0 ; n < rowGroupLength ; index++ ) {
        if ( isNullArray[index] ) {
          allocator.setNull( index + startIndex );
          continue;
        }
        allocator.setFromDictionary( index + startIndex , i , dic );
        n++;
      }
      currentStart += binaryLength;
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

}
