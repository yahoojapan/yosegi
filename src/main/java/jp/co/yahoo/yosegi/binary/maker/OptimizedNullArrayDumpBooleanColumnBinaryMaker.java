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
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.BooleanColumnAnalizeResult;
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

public class OptimizedNullArrayDumpBooleanColumnBinaryMaker implements IColumnBinaryMaker {

  private static final BooleanObj TRUE = new BooleanObj( true );
  private static final BooleanObj FALSE = new BooleanObj( false );

  // Metadata layout
  // ColumnStart, nullLength
  private static final int META_LENGTH = Integer.BYTES * 2;

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

    boolean[] isTrueArray = new boolean[column.size()];
    int trueCount = 0;
    int trueMax = 0;
    int falseMax = 0;
    boolean[] isNullArray = new boolean[column.size()];
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
      isTrueArray[rowCount] = ( (PrimitiveCell)cell ).getRow().getBoolean();
      if ( isTrueArray[rowCount] ) {
        trueCount++;
        trueMax = rowCount;
      } else {
        falseMax = rowCount;
      }

      notNullMaxIndex = nullIndex;
      rowCount++;
    }

    int nullLength = NullBinaryEncoder.getBinarySize(
        nullCount , rowCount , nullMaxIndex , notNullMaxIndex );
    int isTrueLength = NullBinaryEncoder.getBinarySize(
        trueCount , rowCount - trueCount , trueMax , falseMax );

    int binaryLength = 
        META_LENGTH
        + nullLength
        + isTrueLength;
    byte[] binaryRaw = new byte[binaryLength];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binaryRaw , 0 , binaryRaw.length );
    wrapBuffer.putInt( startIndex );
    wrapBuffer.putInt( nullLength );

    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH ,
        nullLength ,
        isNullArray ,
        nullCount ,
        rowCount ,
        nullMaxIndex ,
        notNullMaxIndex );

    NullBinaryEncoder.toBinary(
        binaryRaw ,
        META_LENGTH + nullLength ,
        isTrueLength ,
        isTrueArray ,
        trueCount ,
        rowCount - trueCount ,
        trueMax ,
        falseMax );

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
        ColumnType.BOOLEAN ,
        column.size() ,
        binaryRaw.length ,
        Byte.BYTES * rowCount ,
        -1 ,
        binary ,
        0 ,
        binary.length ,
        null );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return analizeResult.getColumnSize();
  }

  @Override
  public LoadType getLoadType(final ColumnBinary columnBinary, final int loadSize) {
    return LoadType.SEQUENTIAL;
  }

  private void loadFromColumnBinary(final ColumnBinary columnBinary, final ISequentialLoader loader)
      throws IOException {
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);
    int startIndex = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int isTrueLength = binary.length - META_LENGTH - nullLength;

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullLength);

    boolean[] isTrueArray =
        NullBinaryEncoder.toIsNullArray(binary, META_LENGTH + nullLength, isTrueLength);

    int index = 0;
    for (; index < startIndex; index++) {
      loader.setNull(index);
    }
    int isTrueIndex = 0;
    for (int i = 0; i < isNullArray.length; i++, index++) {
      if (isNullArray[i]) {
        loader.setNull(index);
      } else {
        loader.setBoolean(index, isTrueArray[isTrueIndex]);
        isTrueIndex++;
      }
    }
    // NOTE: null padding up to load size
    for (int i = index; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  private void loadFromExpandColumnBinary(
      final ColumnBinary columnBinary, final ISequentialLoader loader) throws IOException {
    ICompressor compressor = FindCompressor.get(columnBinary.compressorClassName);
    byte[] binary =
        compressor.decompress(
            columnBinary.binary, columnBinary.binaryStart, columnBinary.binaryLength);
    ByteBuffer wrapBuffer = ByteBuffer.wrap(binary, 0, binary.length);
    int startIndex = wrapBuffer.getInt();
    int nullLength = wrapBuffer.getInt();
    int isTrueLength = binary.length - META_LENGTH - nullLength;

    boolean[] isNullArray = NullBinaryEncoder.toIsNullArray(binary, META_LENGTH, nullLength);

    boolean[] isTrueArray =
        NullBinaryEncoder.toIsNullArray(binary, META_LENGTH + nullLength, isTrueLength);

    // NOTE: repetitions check
    //   LoadSize is less than real size if repetitions include negative number.
    //   It is possible to be thrown ArrayIndexOutOfBoundsException.
    for (int i = 0; i < columnBinary.repetitions.length; i ++) {
      if (columnBinary.repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
    }

    // NOTE:
    //   Set: currentIndex, value
    int lastIndex = startIndex + isNullArray.length - 1;
    int currentIndex = 0;
    int isTrueIndex = 0;
    for (int i = 0; i < columnBinary.repetitions.length; i++) {
      if (columnBinary.repetitions[i] == 0) {
        if (i >= startIndex && i <= lastIndex && !isNullArray[i - startIndex]) {
          // NOTE: read skip
          isTrueIndex++;
        }
        continue;
      }
      if (i > lastIndex || i < startIndex || isNullArray[i - startIndex]) {
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
      } else {
        boolean value = isTrueArray[isTrueIndex];
        for (int j = 0; j < columnBinary.repetitions[i]; j++) {
          loader.setBoolean(currentIndex, value);
          currentIndex++;
        }
        isTrueIndex++;
      }
    }
  }

  @Override
  public void load(final ColumnBinary columnBinary, final ILoader loader) throws IOException {
    if (loader.getLoaderType() != LoadType.SEQUENTIAL) {
      throw new IOException("Loader type is not SEQUENTIAL.");
    }
    if (columnBinary.isSetLoadSize) {
      loadFromExpandColumnBinary(columnBinary, (ISequentialLoader) loader);
    } else {
      loadFromColumnBinary(columnBinary, (ISequentialLoader) loader);
    }
    loader.finish();
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }
}
