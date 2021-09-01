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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OptimizedNullArrayStringColumnBinaryMaker implements IColumnBinaryMaker {

  // Metadata layout
  // byteOrder, ColumnStart, rowCount, minLength , maxLength ,
  // dicSize , nullLength, indexLength, lengthByteLength
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
    Map<String,Integer> dicMap = new HashMap<String,Integer>();
    byte[][] dicArray = new byte[column.size()][];
    int totalLength = 0;
    int logicalDataLength = 0;
    int[] indexArray = new int[column.size()];
    boolean[] isNullArray = new boolean[column.size()];

    DetermineMinMax<String> detemineMinMax = DetermineMinMaxFactory.createString();
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

    for ( int i = startIndex,arrayIndex = 0 ; i < column.size() ; i++ ,arrayIndex++) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        nullCount++;
        nullMaxIndex = arrayIndex;
        isNullArray[arrayIndex] = true;
        continue;
      }
      PrimitiveCell byteCell = (PrimitiveCell) cell;
      String target = byteCell.getRow().getString();
      if ( target == null ) {
        nullCount++;
        nullMaxIndex = arrayIndex;
        isNullArray[arrayIndex] = true;
        continue;
      }

      byte[] obj = byteCell.getRow().getBytes();
      if ( ! dicMap.containsKey( target ) ) {
        detemineMinMax.set( target );
        lengthMinMax.set( obj.length );
        int dicIndex = dicMap.size();
        dicMap.put( target , dicIndex );
        dicArray[dicIndex] = obj;
        totalLength += obj.length;
      }
      logicalDataLength += Integer.BYTES + obj.length;
      indexArray[rowCount] = dicMap.get( target );
      notNullMaxIndex = arrayIndex;
      rowCount++;
    }

    if ( nullCount == 0
        && detemineMinMax.getMin().equals( detemineMinMax.getMax() )
        && startIndex == 0 ) {
      return ConstantColumnBinaryMaker.createColumnBinary(
          new StringObj( detemineMinMax.getMin() ) , column.getColumnName() , column.size() );
    }

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicMap.size() );

    int indexLength = indexConverter.calcBinarySize( rowCount );

    int lengthByteLength = 0;
    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter(
            lengthMinMax.getMin() , lengthMinMax.getMax() );
    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      lengthByteLength = lengthConverter.calcBinarySize( dicMap.size() );
    }

    int dicLength = totalLength;

    int nullLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );

    byte[] binaryRaw =
        new byte[ META_LENGTH + nullLength + indexLength + lengthByteLength + dicLength ];

    ByteOrder order = ByteOrder.nativeOrder();

    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw );
    wrapBuffer.put( order == ByteOrder.BIG_ENDIAN ? (byte)0 : (byte)1 );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( rowCount );
    wrapBuffer.putInt( lengthMinMax.getMin() );
    wrapBuffer.putInt( lengthMinMax.getMax() );
    wrapBuffer.putInt( dicMap.size() );
    wrapBuffer.putInt( nullLength );
    wrapBuffer.putInt( indexLength );
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
    IWriteSupporter indexWriter = indexConverter.toWriteSuppoter(
        rowCount , binaryRaw , META_LENGTH + nullLength , indexLength  );
    for ( int i = 0 ; i < rowCount ; i++ ) {
      indexWriter.putInt( indexArray[i] );
    }

    if ( ! lengthMinMax.getMin().equals( lengthMinMax.getMax() ) ) {
      IWriteSupporter lengthWriter = lengthConverter.toWriteSuppoter(
          dicMap.size() , binaryRaw , META_LENGTH + nullLength + indexLength , lengthByteLength  );
      for ( int i = 0 ; i < dicMap.size(); i++ ) {
        lengthWriter.putInt( dicArray[i].length );
      }
    }

    ByteBuffer valueBuffer = ByteBuffer.wrap(
        binaryRaw , META_LENGTH + nullLength + indexLength + lengthByteLength , totalLength );
    for ( int i = 0 ; i < dicMap.size() ; i++ ) {
      valueBuffer.put( dicArray[i] );
    }

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressBinary = currentConfig.compressorClass.compress(
        binaryRaw , 0 , binaryRaw.length , compressResult );

    int minCharLength = Character.BYTES * detemineMinMax.getMin().length();
    int maxCharLength = Character.BYTES * detemineMinMax.getMax().length();
    int headerSize = Integer.BYTES + minCharLength + Integer.BYTES + maxCharLength;

    byte[] binary = new byte[headerSize + compressBinary.length];
    wrapBuffer = ByteBuffer.wrap( binary , 0 , binary.length );
    ByteBuffer binaryWrapBuffer = ByteBuffer.wrap( binary );
    binaryWrapBuffer.putInt( minCharLength );
    binaryWrapBuffer.asCharBuffer().put( detemineMinMax.getMin() );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + minCharLength );
    binaryWrapBuffer.putInt( maxCharLength );
    binaryWrapBuffer.asCharBuffer().put( detemineMinMax.getMax() );
    binaryWrapBuffer.position( binaryWrapBuffer.position() + maxCharLength );
    binaryWrapBuffer.put( compressBinary );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        column.getColumnType() ,
        column.size() ,
        binaryRaw.length ,
        logicalDataLength ,
        dicMap.size() ,
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

    int nullLength =
        NullBinaryEncoder.getBinarySize( nullCount , notNullCount , maxIndex , maxIndex );
    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , analizeResult.getUniqCount() );


    int indexLength = indexConverter.calcBinarySize( notNullCount );

    int minLength = stringAnalizeResult.getMinUtf8Bytes();
    int maxLength = stringAnalizeResult.getMaxUtf8Bytes();
    int lengthBinaryLength = 0;

    NumberToBinaryUtils.IIntConverter lengthConverter =
        NumberToBinaryUtils.getIntConverter( minLength , maxLength );
    if ( ! ( minLength == maxLength ) ) {
      lengthBinaryLength = lengthConverter.calcBinarySize( notNullCount );
    }

    int dicLength = stringAnalizeResult.getUniqUtf8ByteSize();

    return META_LENGTH + nullLength + indexLength + lengthBinaryLength + dicLength;
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    int loadCount = columnBinary.rowCount;
    if ( columnBinary.loadIndex != null ) {
      loadCount = columnBinary.loadIndex.length;
    }
    return new YosegiLoaderFactory().create(
        columnBinary , loadCount );
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary , final int loadSize ) {
    return LoadType.DICTIONARY;
  }

  private byte[] getDecompressBinary( final ColumnBinary columnBinary ) throws IOException {
    ByteBuffer rawBuffer = ByteBuffer.wrap(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    int minBinaryLength = rawBuffer.getInt();
    rawBuffer.position( rawBuffer.position() + minBinaryLength );

    int maxBinaryLength = rawBuffer.getInt();
    rawBuffer.position( rawBuffer.position() + maxBinaryLength );

    int headerSize = Integer.BYTES + minBinaryLength + Integer.BYTES + maxBinaryLength;

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress(
        columnBinary.binary ,
        columnBinary.binaryStart + headerSize ,
        columnBinary.binaryLength - headerSize );
    return binary;

  }

  private class BinaryMeta {
    public final ByteOrder order;
    public final int startIndex;
    public final int rowCount;
    public final int minLength;
    public final int maxLength;
    public final int dicSize;
    public final int nullLength;
    public final int indexLength;
    public final int lengthBinaryLength;

    private BinaryMeta( final byte[] binary , final int start , final int length ) {
      ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );

      order = wrapBuffer.get() == (byte)0
          ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      startIndex = wrapBuffer.getInt();
      rowCount = wrapBuffer.getInt();
      minLength = wrapBuffer.getInt();
      maxLength = wrapBuffer.getInt();
      dicSize = wrapBuffer.getInt();
      nullLength = wrapBuffer.getInt();
      indexLength = wrapBuffer.getInt();
      lengthBinaryLength = wrapBuffer.getInt();
    }

  }

  private void loadFromColumnBinary(
      final ColumnBinary columnBinary , final IDictionaryLoader loader ) throws IOException {
    byte[] binary = getDecompressBinary( columnBinary );
    BinaryMeta meta = new BinaryMeta( binary , 0 , binary.length );
    int dicLength = binary.length
                    - META_LENGTH
                    - meta.nullLength
                    - meta.indexLength
                    - meta.lengthBinaryLength;

    IReadSupporter lengthReader;
    if ( meta.minLength == meta.maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( meta.minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( meta.minLength , meta.maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + meta.nullLength + meta.indexLength ,
          meta.lengthBinaryLength );
    }
    loader.createDictionary( meta.dicSize );

    int currentStart =
        META_LENGTH + meta.nullLength + meta.indexLength + meta.lengthBinaryLength;
    for ( int i = 0 ; i < meta.dicSize ; i++ ) {
      int currentLength = lengthReader.getInt();
      loader.setBytesToDic( i , binary , currentStart , currentLength );
      currentStart += currentLength;
    }

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , meta.dicSize );
    IReadSupporter indexReader = indexConverter.toReadSupporter(
        binary , META_LENGTH + meta.nullLength , meta.indexLength );
    for ( int i = 0; i < meta.startIndex ; i++ ) {
      loader.setNull( i );
    }

    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , meta.nullLength );

    for ( int i = 0 ; i < isNullArray.length ; i++ ) {
      if ( isNullArray[i]  ) {
        loader.setNull( i + meta.startIndex );
      } else {
        loader.setDictionaryIndex( i + meta.startIndex , indexReader.getInt() );
      }
    }
    for ( int i = isNullArray.length + meta.startIndex ; i < loader.getLoadSize() ; i++ ) {
      loader.setNull( i );
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary , final IDictionaryLoader loader ) throws IOException {
    byte[] binary = getDecompressBinary( columnBinary );
    BinaryMeta meta = new BinaryMeta( binary , 0 , binary.length );
    int dicLength = binary.length
                    - META_LENGTH
                    - meta.nullLength
                    - meta.indexLength
                    - meta.lengthBinaryLength;

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , meta.dicSize );
    IReadSupporter indexReader = indexConverter.toReadSupporter(
        binary , META_LENGTH + meta.nullLength , meta.indexLength );
    boolean[] isNullArray =
        NullBinaryEncoder.toIsNullArray( binary , META_LENGTH , meta.nullLength );
    boolean[] isNeedDictionary = new boolean[meta.dicSize];
    int[] dicIndexList = new int[isNullArray.length];
    for ( int i = 0 ; i < isNullArray.length ; i++ ) {
      if ( ! isNullArray[i]  ) {
        dicIndexList[i] = indexReader.getInt();
      }
    }

    int currentLoadIndex = 0;
    int[] newDicIndexList = new int[meta.dicSize];
    int needDicCount = 0;
    for ( int loadIndex : columnBinary.loadIndex ) {
      if ( loadIndex < 0 ) {
        throw new IOException(
            "Index must be greater than 0." );
      } else if ( loadIndex < currentLoadIndex ) {
        throw new IOException( "Index must be equal to or greater than the previous number." );
      }
      if ( meta.startIndex + isNullArray.length <= loadIndex ) {
        break;
      }
      currentLoadIndex = loadIndex;
      if ( loadIndex < meta.startIndex || isNullArray[loadIndex - meta.startIndex] ) {
        continue;
      }
      if ( ! isNeedDictionary[dicIndexList[loadIndex - meta.startIndex]] ) {
        isNeedDictionary[dicIndexList[loadIndex - meta.startIndex]] = true;
        newDicIndexList[dicIndexList[loadIndex - meta.startIndex]] = needDicCount;
        needDicCount++;
      }
    }
    loader.createDictionary( needDicCount );

    IReadSupporter lengthReader;
    if ( meta.minLength == meta.maxLength ) {
      lengthReader = NumberToBinaryUtils.getFixedIntConverter( meta.minLength );
    } else {
      NumberToBinaryUtils.IIntConverter lengthConverter =
          NumberToBinaryUtils.getIntConverter( meta.minLength , meta.maxLength );
      lengthReader = lengthConverter.toReadSupporter(
          binary ,
          META_LENGTH + meta.nullLength + meta.indexLength ,
          meta.lengthBinaryLength );
    }
    int addDicCount = 0;
    int currentStart =
        META_LENGTH + meta.nullLength + meta.indexLength + meta.lengthBinaryLength;
    for ( int i = 0 ; i < meta.dicSize ; i++ ) {
      int currentLength = lengthReader.getInt();
      if ( isNeedDictionary[i] ) {
        loader.setBytesToDic( addDicCount , binary , currentStart , currentLength );
        addDicCount++;
      }
      currentStart += currentLength;
    }

    int currentColumnIndex = 0;
    for ( int loadIndex : columnBinary.loadIndex ) {
      if ( meta.startIndex + isNullArray.length <= loadIndex
          || loadIndex < meta.startIndex 
          || isNullArray[loadIndex - meta.startIndex] ) {
        loader.setNull( currentColumnIndex );
      } else {
        loader.setDictionaryIndex(
            currentColumnIndex , newDicIndexList[dicIndexList[loadIndex - meta.startIndex]] );
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

    ByteOrder order = wrapBuffer.get() == (byte)0
        ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int startIndex = wrapBuffer.getInt();
    final int rowCount = wrapBuffer.getInt();
    int minLength = wrapBuffer.getInt();
    int maxLength = wrapBuffer.getInt();
    int dicSize = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int indexLength = wrapBuffer.getInt();
    int lengthBinaryLength = wrapBuffer.getInt();
    int dicLength = binary.length - META_LENGTH - nullLength - indexLength - lengthBinaryLength;

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
          META_LENGTH + nullLength + indexLength ,
          lengthBinaryLength );
    }

    IDictionary dic = allocator.createDictionary( dicSize );
    int currentStart = META_LENGTH + nullLength + indexLength + lengthBinaryLength;
    for ( int i = 0 ; i < dicSize ; i++ ) {
      int currentLength = lengthReader.getInt();
      dic.setBytes( i , binary , currentStart , currentLength );
      currentStart += currentLength;
    }

    allocator.setValueCount( startIndex + isNullArray.length );

    NumberToBinaryUtils.IIntConverter indexConverter =
        NumberToBinaryUtils.getIntConverter( 0 , dicSize );
    IReadSupporter indexReader =
        indexConverter.toReadSupporter( binary , META_LENGTH + nullLength , indexLength );
    for ( int i = 0; i < startIndex ; i++ ) {
      allocator.setNull( i );
    }
    for ( int i = 0 ; i < isNullArray.length ; i++ ) {
      if ( isNullArray[i]  ) {
        allocator.setNull( i + startIndex );
      } else {
        allocator.setFromDictionary( i + startIndex , indexReader.getInt() , dic );
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

}
