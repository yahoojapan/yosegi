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

public class TestIndexNullBinaryEncoder {

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_allNull() throws IOException {
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
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 10 , 0 , 9 , 0 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 10 , 0 , 9 , 0 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_notNull() throws IOException {
    boolean[] original = new boolean[]{
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false
    };
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 0 , 10 , 0 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 0 , 10 , 0 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_headIsNull() throws IOException {
    boolean[] original = new boolean[]{
      true,
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false
    };
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 1 , 9 , 0 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 1 , 9 , 0 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastIsNull() throws IOException {
    boolean[] original = new boolean[]{
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      false,
      true
    };
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 1 , 9 , 9 , 8 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 1 , 9 , 9 , 8 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_centerIsNull() throws IOException {
    boolean[] original = new boolean[]{
      false,
      false,
      false,
      false,
      true,
      true,
      false,
      false,
      false,
      false
    };
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 2 , 8 , 5 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 2 , 8 , 5 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_stripe() throws IOException {
    boolean[] original = new boolean[]{
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
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 5 , 5 , 9 , 8 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 5 , 5 , 9 , 8 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_headIsNotNull() throws IOException {
    boolean[] original = new boolean[]{
      false,
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
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 9 , 1 , 9 , 0 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 9 , 1 , 9 , 0 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_lastIsNotNull() throws IOException {
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
      false
    };
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 9 , 1 , 8 , 9 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 9 , 1 , 8 , 9 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

  @Test
  public void T_encodeAndDecode_equalsOriginalArray_centerIsNotNull() throws IOException {
    boolean[] original = new boolean[]{
      true,
      true,
      true,
      true,
      false,
      false,
      true,
      true,
      true,
      true
    };
    INullBinaryEncoder encoder = new IndexNullBinaryEncoder();
    int binarySize = encoder.getBinarySize( 8 , 2 , 9 , 5 );
    byte[] binary = new byte[binarySize];
    encoder.toBinary( binary , 0 , binarySize , original , 8 , 2 , 9 , 5 );
    boolean[] result = encoder.toIsNullArray( binary , 0 , binary.length );
    assertEquals( original.length , result.length );
    for ( int i = 0 ; i < original.length ; i++ ) {
      assertEquals( original[i] , result[i] );
    }
  }

}
