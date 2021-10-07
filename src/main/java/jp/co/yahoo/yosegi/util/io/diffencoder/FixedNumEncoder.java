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

package jp.co.yahoo.yosegi.util.io.diffencoder;

import jp.co.yahoo.yosegi.inmemory.IDictionary;
import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.io.IOException;
import java.nio.ByteOrder;

public class FixedNumEncoder implements INumEncoder {

  private final long min;

  /**
   * Init.
   */
  public FixedNumEncoder( final long min , final long max ) throws IOException {
    if ( min != max ) {
      throw new IOException( "min is not equal max." );
    }
    this.min = min;
  }

  @Override
  public int calcBinarySize( final int rows ) {
    return Long.BYTES;
  }

  @Override
  public void toBinary(
      final long[] longArray ,
      final byte[] buffer ,
      final int start ,
      final int rows ,
      final ByteOrder order ) throws IOException {
    IWriteSupporter wrapBuffer = ByteBufferSupporterFactory
        .createWriteSupporter( buffer , start , calcBinarySize( rows ) , order );
    wrapBuffer.putLong( min );
  }

  @Override
  public void toBinary(
      final Long[] longArray ,
      final byte[] buffer ,
      final int start ,
      final int rows ,
      final ByteOrder order ) throws IOException {
    IWriteSupporter wrapBuffer = ByteBufferSupporterFactory
        .createWriteSupporter( buffer , start , calcBinarySize( rows ) , order );
    wrapBuffer.putLong( min );
  }

  @Override
  public void setDictionary(
      final byte[] buffer ,
      final int start ,
      final int rows ,
      final ByteOrder order ,
      final IDictionary dic ) throws IOException {
    IReadSupporter wrapBuffer = ByteBufferSupporterFactory
        .createReadSupporter( buffer , start , calcBinarySize( rows ) , order );
    long num = wrapBuffer.getLong();
    for ( int i = 0 ; i < rows ; i++ ) {
      dic.setLong( i , num );
    }
  }

  @Override
  public void setSequentialLoader(
      final byte[] buffer,
      final int start,
      final int rows,
      final boolean[] isNullArray,
      final ByteOrder order,
      final ISequentialLoader loader,
      final int startIndex)
      throws IOException {
    IReadSupporter wrapBuffer =
        ByteBufferSupporterFactory.createReadSupporter(buffer, start, calcBinarySize(rows), order);
    long value = wrapBuffer.getLong();
    int index = 0;
    for (; index < startIndex; index++) {
      loader.setNull(index);
    }
    for (int i = 0; i < isNullArray.length; i++, index++) {
      if (isNullArray[i]) {
        loader.setNull(index);
      } else {
        loader.setLong(index, value);
      }
    }
    // NOTE: null padding up to load size
    for (int i = index; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  @Override
  public void setDictionaryLoader(
      final byte[] buffer,
      final int start,
      final int rows,
      final boolean[] isNullArray,
      final ByteOrder order,
      final IDictionaryLoader loader,
      final int startIndex,
      final int[] loadIndexArray)
      throws IOException {
    IReadSupporter wrapBuffer =
        ByteBufferSupporterFactory.createReadSupporter(buffer, start, calcBinarySize(rows), order);
    long value = wrapBuffer.getLong();

    // NOTE: Calculate dictionarySize
    int dictionarySize = 0;
    int previousLoadIndex = -1;
    int lastIndex = startIndex + isNullArray.length - 1;
    for (int loadIndex : loadIndexArray) {
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
    for (int loadIndex : loadIndexArray) {
      if (loadIndex > lastIndex) {
        break;
      }
      if (loadIndex < startIndex || isNullArray[loadIndex - startIndex]) {
        loader.setNull(loadIndexArrayOffset);
      } else {
        if (previousLoadIndex != loadIndex) {
          dictionaryIndex++;
          loader.setLongToDic(dictionaryIndex, value);
        }
        loader.setDictionaryIndex(loadIndexArrayOffset, dictionaryIndex);
      }
      previousLoadIndex = loadIndex;
      loadIndexArrayOffset++;
    }

    // NOTE: null padding up to load size
    for (int i = loadIndexArrayOffset; i < loader.getLoadSize(); i++) {
      loader.setNull(i);
    }
  }

  @Override
  public void setDictionaryLoader(
      final byte[] buffer,
      final int start,
      final int rows,
      final boolean[] isNullArray,
      final ByteOrder order,
      final IDictionaryLoader loader,
      final int startIndex,
      final int[] repetitions,
      final int loadSize)
      throws IOException {
    IReadSupporter wrapBuffer =
        ByteBufferSupporterFactory.createReadSupporter(buffer, start, calcBinarySize(rows), order);

    // NOTE: repetitions check
    //   LoadSize is less than real size if repetitions include negative number.
    //   It is possible to be thrown ArrayIndexOutOfBoundsException.
    for (int i = 0; i < repetitions.length; i ++) {
      if (repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
    }

    // NOTE: Calculate dictionarySize
    int dictionarySize = 1;
    loader.createDictionary(dictionarySize);

    // NOTE:
    //   Set value to dict: dictionaryIndex, value
    int dictionaryIndex = 0;
    long value = wrapBuffer.getLong();
    loader.setLongToDic(dictionaryIndex, value);

    // NOTE:
    //   Set dictionaryIndex: currentIndex, dictionaryIndex
    int currentIndex = 0;
    int lastIndex = startIndex + isNullArray.length - 1;
    for (int i = 0; i < repetitions.length; i++) {
      if (repetitions[i] < 0) {
        throw new IOException("Repetition must be equal to or greater than 0.");
      }
      if (repetitions[i] == 0) {
        continue;
      }
      if (i > lastIndex || i < startIndex || isNullArray[i - startIndex]) {
        for (int j = 0; j < repetitions[i]; j++) {
          loader.setNull(currentIndex);
          currentIndex++;
        }
      } else {
        for (int j = 0; j < repetitions[i]; j++) {
          loader.setDictionaryIndex(currentIndex, dictionaryIndex);
          currentIndex++;
        }
      }
    }
  }
}
