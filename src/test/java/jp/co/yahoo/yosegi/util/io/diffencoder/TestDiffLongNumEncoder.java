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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import java.io.IOException;
import java.nio.ByteOrder;

public class TestDiffLongNumEncoder {

  @Test
  public void T_calcBinarySize_equals() throws IOException {
    DiffLongNumEncoder encoder = new DiffLongNumEncoder( (long)Byte.MIN_VALUE , (long)Byte.MAX_VALUE );
    assertEquals( encoder.calcBinarySize( 10 ) , 12 );
  }

  @Test
  public void T_setDictionary_equalsSetValue() throws IOException {
    DiffLongNumEncoder encoder = new DiffLongNumEncoder( (long)Byte.MAX_VALUE , (long)Byte.MIN_VALUE );
    byte[] buffer = new byte[encoder.calcBinarySize(10)];
    long[] valueArray = new long[]{
      0L,
      (long)Byte.MAX_VALUE,
      (long)Byte.MIN_VALUE,
      1L,
      2L,
      3L,
      4L,
      5L,
      6L,
      7L
    };
    encoder.toBinary( valueArray , buffer , 0 , 10 , ByteOrder.nativeOrder() );
    EncoderTestDictionary dic = new EncoderTestDictionary( 10 );
    encoder.setDictionary( buffer , 0 , 10 , ByteOrder.nativeOrder() , dic );
    long[] result = dic.getLongArray();
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( result[i] , valueArray[i] );
    }
  }

}
