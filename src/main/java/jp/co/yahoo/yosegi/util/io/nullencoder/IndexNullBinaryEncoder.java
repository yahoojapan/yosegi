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

package jp.co.yahoo.yosegi.util.io.nullencoder;

import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexNullBinaryEncoder implements INullBinaryEncoder {

  public static final int HEADER_SIZE = Byte.BYTES + Integer.BYTES * 3;

  @Override
  public int getBinarySize(
      final int nullCount ,
      final int notNullCount ,
      final int maxNullIndex ,
      final int maxNotNullIndex ) {
    if ( notNullCount < nullCount ) {
      NumberToBinaryUtils.IIntConverter converter
          = NumberToBinaryUtils.getIntConverter( 0 , maxNotNullIndex );
      return converter.calcBinarySize( notNullCount ) + HEADER_SIZE;
    } else {
      NumberToBinaryUtils.IIntConverter converter
          = NumberToBinaryUtils.getIntConverter( 0 , maxNullIndex );
      return converter.calcBinarySize( nullCount ) + HEADER_SIZE;
    }
  }

  @Override
  public void toBinary(
      final byte[] binary,
      final int start,
      final int length,
      final boolean[] isNullArray,
      final int nullCount ,
      final int notNullCount ,
      final int maxNullIndex ,
      final int maxNotNullIndex ) throws IOException {
    boolean saveTarget;
    int rows;
    int maxIndex;
    if ( notNullCount < nullCount ) {
      saveTarget = false;
      rows = notNullCount;
      maxIndex = maxNotNullIndex;
    } else {
      saveTarget = true;
      rows = nullCount;
      maxIndex = maxNullIndex;
    }
    int isNullArrayLength = nullCount + notNullCount;
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    wrapBuffer.put( ( saveTarget ) ? (byte)1 : (byte)0 );
    wrapBuffer.putInt( isNullArrayLength );
    wrapBuffer.putInt( rows );
    wrapBuffer.putInt( maxIndex );

    NumberToBinaryUtils.IIntConverter converter
        = NumberToBinaryUtils.getIntConverter( 0 , maxIndex );
    int binaryLength = converter.calcBinarySize( rows );
    IWriteSupporter writer =
        converter.toWriteSuppoter( rows , binary , HEADER_SIZE + start , binaryLength );
    for ( int i = 0 ; i < isNullArrayLength ; i++ ) {
      if ( isNullArray[i] == saveTarget ) {
        writer.putInt( i );
      }
    }
  }

  @Override
  public boolean[] toIsNullArray(
      final byte[] binary , final int start , final int length ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    boolean saveTarget = ( wrapBuffer.get() == 1 );
    int arrayLength = wrapBuffer.getInt();
    int rows = wrapBuffer.getInt();
    int maxIndex = wrapBuffer.getInt();
    IReadSupporter reader = NumberToBinaryUtils.getIntConverter( 0 , maxIndex )
        .toReadSupporter( binary , start + HEADER_SIZE , length - HEADER_SIZE );
    boolean[] result = new boolean[arrayLength];
    int currentIndex = 0;
    for ( int i = 0 ; i < rows ; i++ ) {
      int nextIndex = reader.getInt();
      for ( ; currentIndex < nextIndex ; currentIndex++ ) {
        result[currentIndex] = !saveTarget;
      }
      result[currentIndex] = saveTarget;
      currentIndex++;
    }
    for ( ; currentIndex < result.length ; currentIndex++ ) {
      result[currentIndex] = !saveTarget;
    }
    return result;
  }

}
