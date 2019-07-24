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

public class TestBitNullBinaryEncoder {

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_stripe() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 5 , 5 , 8 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 5 , 5 , 8 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_emptyArray() throws IOException {
    boolean[] original = new boolean[0];
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 0 , 0 , 0 , 0 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 0 , 0 , 0 , 0 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_divisibleByBitLength() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 16 , original.length );
    int binarySize = encoder.getBinarySize( 8 , 8 , 14 , 15 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 8 , 8 , 14 , 15 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength7() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 15 , original.length );
    int binarySize = encoder.getBinarySize( 8 , 7 , 14 , 13 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 8 , 7 , 14 , 13 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength6() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 14 , original.length );
    int binarySize = encoder.getBinarySize( 7 , 7 , 12 , 13 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 7 , 7 , 12 , 13 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength5() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 13 , original.length );
    int binarySize = encoder.getBinarySize( 7 , 6 , 12 , 11 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 7 , 6 , 12 , 11 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength4() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 12 , original.length );
    int binarySize = encoder.getBinarySize( 6 , 6 , 10 , 11 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 6 , 6 , 10 , 11 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength3() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 11 , original.length );
    int binarySize = encoder.getBinarySize( 6 , 5 , 10 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 6 , 5 , 10 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength2() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 10 , original.length );
    int binarySize = encoder.getBinarySize( 5 , 5 , 8 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 5 , 5 , 8 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastLength1() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      true,
      false,
      true,
      false,
      true,
      false,
      true
    };
    INullBinaryEncoder encoder = new BitNullBinaryEncoder();
    assertEquals( 9 , original.length );
    int binarySize = encoder.getBinarySize( 5 , 4 , 8 , 7 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 5 , 4 , 8 , 7 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

}
