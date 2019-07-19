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

package jp.co.yahoo.yosegi.util.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class NullBitEncoder {

  public static final int HEADER_SIZE = Integer.BYTES;
  public static final int BIT_LENGTH = 8;

  private NullBitEncoder() {}

  private static int binarySize( final int arraySize ) {
    double sizeDouble = Math.ceil(
        Integer.valueOf( arraySize ).doubleValue() / Integer.valueOf( BIT_LENGTH ).doubleValue() );
    return Double.valueOf( sizeDouble ).intValue();
  }

  /**
   * Get binary size.
   **/
  public static int getBinarySize( boolean[] isNullArray ) {
    if ( isNullArray.length == 0 ) {
      return HEADER_SIZE;
    }
    return binarySize( isNullArray.length )  + HEADER_SIZE;
  }

  /**
   * Convert to binary.
   */
  public static byte[] toBinary( final boolean[] isNullArray ) throws IOException {
    byte[] result = new byte[getBinarySize( isNullArray )];
    ByteBuffer.wrap( result ).putInt( isNullArray.length );
    if ( isNullArray.length == 0 ) {
      return result;
    }
    boolean[] buffer = new boolean[BIT_LENGTH];
    for ( int i = 0,n = HEADER_SIZE ; i < isNullArray.length ; i += BIT_LENGTH , n++ ) {
      int copyLength = BIT_LENGTH;
      if ( isNullArray.length < ( i + BIT_LENGTH ) ) {
        copyLength = isNullArray.length - i;
      }
      System.arraycopy( isNullArray , i , buffer , 0 , copyLength );
      result[n] = (byte)(
          ( (buffer[0]) ? 1 << 7 : 0 )
          | ( (buffer[1]) ? 1 << 6 : 0 )
          | ( (buffer[2]) ? 1 << 5 : 0 )
          | ( (buffer[3]) ? 1 << 4 : 0 )
          | ( (buffer[4]) ? 1 << 3 : 0 )
          | ( (buffer[5]) ? 1 << 2 : 0 )
          | ( (buffer[6]) ? 1 << 1 : 0 )
          | ( (buffer[7]) ? 1 : 0 ) );
    }
    return result;
  }

  /**
   * Convert to isNullArray.
   */
  public static boolean[] toIsNullArray(
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
