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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NullBinaryEncoder {

  private static final Map<Byte,INullBinaryEncoder> ENCODERS
      = new HashMap<Byte,INullBinaryEncoder>();

  static {
    ENCODERS.put( Byte.valueOf( (byte)0 ) , new BitNullBinaryEncoder() );
    ENCODERS.put( Byte.valueOf( (byte)1 ) , new IndexNullBinaryEncoder() );
  }

  /**
   * Get encoder key.
   */
  public static Byte getEncoder(
      final int nullCount ,
      final int notNullCount ,
      final int maxNullIndex ,
      final int maxNotNullIndex ) {
    INullBinaryEncoder currentEncoder = null;
    int currentBinarySize = Integer.MAX_VALUE;
    Byte currentKey = null;
    for ( Map.Entry<Byte,INullBinaryEncoder> entry : ENCODERS.entrySet() ) {
      INullBinaryEncoder encoder = entry.getValue();
      int binarySize =
          encoder.getBinarySize( nullCount , notNullCount , maxNullIndex , maxNotNullIndex );
      if ( currentEncoder == null || binarySize < currentBinarySize ) {
        currentEncoder = encoder;
        currentKey = entry.getKey();
        currentBinarySize = binarySize;
      }
    }
    return currentKey;
  }

  /**
   * Get binary size.
   **/
  public static int getBinarySize(
      final int nullCount ,
      final int notNullCount ,
      final int maxNullIndex ,
      final int maxNotNullIndex ) {
    Byte encoderKey =
        getEncoder( nullCount , notNullCount , maxNullIndex , maxNotNullIndex );
    INullBinaryEncoder encoder = ENCODERS.get( encoderKey );
    return encoder.getBinarySize(
        nullCount , notNullCount , maxNullIndex , maxNotNullIndex ) + Byte.BYTES;
  }

  /**
   * boolean array to binary.
   **/
  public static void toBinary(
      final byte[] binary,
      final int start,
      final int length,
      final boolean[] isNullArray,
      final int nullCount ,
      final int notNullCount ,
      final int maxNullIndex ,
      final int maxNotNullIndex ) throws IOException {
    Byte encoderKey =
        getEncoder( nullCount , notNullCount , maxNullIndex , maxNotNullIndex );
    INullBinaryEncoder encoder = ENCODERS.get( encoderKey );
    binary[start] = encoderKey.byteValue();
    encoder.toBinary(
        binary ,
        start + Byte.BYTES,
        length - Byte.BYTES,
        isNullArray ,
        nullCount ,
        notNullCount ,
        maxNullIndex ,
        maxNotNullIndex );
  }

  /**
   * binary to isNullArray.
   **/
  public static   boolean[] toIsNullArray(
      final byte[] binary , final int start , final int length ) throws IOException {
    Byte encoderKey = Byte.valueOf( binary[start] );
    INullBinaryEncoder encoder = ENCODERS.get( encoderKey );
    return encoder.toIsNullArray( binary , start + Byte.BYTES , length - Byte.BYTES );
  }

}
