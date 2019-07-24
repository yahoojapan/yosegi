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

import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BitNullBinaryEncoder implements INullBinaryEncoder {

  public static final int HEADER_SIZE = Integer.BYTES;
  public static final int BIT_LENGTH = 8;

  private static int binarySize( final int arraySize ) {
    double sizeDouble = Math.ceil(
        Integer.valueOf( arraySize ).doubleValue() / Integer.valueOf( BIT_LENGTH ).doubleValue() );
    return Double.valueOf( sizeDouble ).intValue();
  }

  @Override
  public int getBinarySize(
      final int nullCount ,
      final int notNullCount ,
      final int maxNullIndex ,
      final int maxNotNullIndex ) {
    int isNullArrayLength = nullCount + notNullCount;
    if ( isNullArrayLength == 0 ) {
      return HEADER_SIZE;
    }
    return binarySize( isNullArrayLength )  + HEADER_SIZE;
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
    int isNullArrayLength = nullCount + notNullCount;
    ByteBuffer.wrap( binary , start , length ).putInt( isNullArrayLength );
    if ( isNullArrayLength == 0 ) {
      return;
    }
    boolean[] buffer = new boolean[BIT_LENGTH];
    for ( int i = 0,n = HEADER_SIZE + start ; i < isNullArrayLength ; i += BIT_LENGTH , n++ ) {
      int copyLength = BIT_LENGTH;
      if ( isNullArrayLength < ( i + BIT_LENGTH ) ) {
        copyLength = isNullArrayLength - i;
      }
      System.arraycopy( isNullArray , i , buffer , 0 , copyLength );
      binary[n] = (byte)(
          ( (buffer[0]) ? 1 << 7 : 0 )
          | ( (buffer[1]) ? 1 << 6 : 0 )
          | ( (buffer[2]) ? 1 << 5 : 0 )
          | ( (buffer[3]) ? 1 << 4 : 0 )
          | ( (buffer[4]) ? 1 << 3 : 0 )
          | ( (buffer[5]) ? 1 << 2 : 0 )
          | ( (buffer[6]) ? 1 << 1 : 0 )
          | ( (buffer[7]) ? 1 : 0 ) );
    }
  }

  @Override
  public boolean[] toIsNullArray(
      final byte[] binary , final int start , final int length ) throws IOException {
    int rows = ByteBuffer.wrap( binary , start , length ).getInt();
    boolean[] result = new boolean[rows];
    if ( rows == 0 ) {
      return result;
    }
    boolean[] buffer = new boolean[BIT_LENGTH];
    for ( int i = 0 , n = start + HEADER_SIZE ; i < result.length ; i += BIT_LENGTH , n++ ) {
      int isNullFlag = NumberToBinaryUtils.getUnsignedByteToInt( binary[n] );
      buffer[0] = ( ( isNullFlag & 1 << 7 ) > 0 );
      buffer[1] = ( ( isNullFlag & 1 << 6 ) > 0 );
      buffer[2] = ( ( isNullFlag & 1 << 5 ) > 0 );
      buffer[3] = ( ( isNullFlag & 1 << 4 ) > 0 );
      buffer[4] = ( ( isNullFlag & 1 << 3 ) > 0 );
      buffer[5] = ( ( isNullFlag & 1 << 2 ) > 0 );
      buffer[6] = ( ( isNullFlag & 1 << 1 ) > 0 );
      buffer[7] = ( ( isNullFlag & 1 ) > 0 );
      int copyLength = BIT_LENGTH;
      if ( result.length <= ( i + BIT_LENGTH ) ) {
        copyLength = result.length - i;
      }
      System.arraycopy( buffer , 0 , result , i , copyLength );
    }
    return result;
  }

}
