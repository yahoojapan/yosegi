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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestNullBinaryEncoder {

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_encoderIs0() throws IOException {
    boolean[] original = new boolean[]{
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true
    };
    assertEquals( Byte.valueOf( (byte)0 ) , NullBinaryEncoder.getEncoder( 10 , 0 , 9 , 0 ) );
    int binarySize = NullBinaryEncoder.getBinarySize( 10 , 0 , 9 , 0 );
    byte[] binary = new byte[binarySize];
    NullBinaryEncoder.toBinary( binary , 0 , binarySize , original , 10 , 0 , 9 , 0 );
    boolean[] result = NullBinaryEncoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_encoderIs1() throws IOException {
    boolean[] original = new boolean[128];
    original[0] = true;
    assertEquals( Byte.valueOf( (byte)1 ) , NullBinaryEncoder.getEncoder( 1 , 127 , 0 , 127 ) );
    int binarySize = NullBinaryEncoder.getBinarySize( 1 , 127 , 0 , 127 );
    byte[] binary = new byte[binarySize];
    NullBinaryEncoder.toBinary( binary , 0 , binarySize , original , 1 , 127 , 0 , 127 );
    boolean[] result = NullBinaryEncoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

}
