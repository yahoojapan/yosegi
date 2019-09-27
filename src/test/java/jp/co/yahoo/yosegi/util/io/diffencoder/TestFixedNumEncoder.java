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

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;

import java.io.IOException;
import java.nio.ByteOrder;

public class TestFixedNumEncoder {

  @Test
  public void T_calcBinarySize_equals() throws IOException {
    FixedNumEncoder encoder = new FixedNumEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    assertEquals( encoder.calcBinarySize( 10 ) , 8 );
  }

  @Test
  public void T_toBinaryAndtoPrimitiveArrayWithPrimitiveLong_equalsSetValue() throws IOException {
    FixedNumEncoder encoder = new FixedNumEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    byte[] buffer = new byte[encoder.calcBinarySize(10)];
    long[] valueArray = new long[]{
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE
    };
    encoder.toBinary( valueArray , buffer , 0 , 10 , ByteOrder.nativeOrder() );
    PrimitiveObject[] result = encoder.toPrimitiveArray( buffer , 0 , 10 , ByteOrder.nativeOrder() );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( result[i].getLong() , valueArray[i] );
    }
  }

  @Test
  public void T_toBinaryAndtoPrimitiveArrayWithJavaLong_equalsSetValue() throws IOException {
    FixedNumEncoder encoder = new FixedNumEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    byte[] buffer = new byte[encoder.calcBinarySize(10)];
    Long[] valueArray = new Long[]{
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE ),
      Long.valueOf( Long.MAX_VALUE )
    };
    encoder.toBinary( valueArray , buffer , 0 , 10 , ByteOrder.nativeOrder() );
    PrimitiveObject[] result = encoder.toPrimitiveArray( buffer , 0 , 10 , ByteOrder.nativeOrder() );
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( result[i].getLong() , valueArray[i].longValue() );
    }
  }

  @Test
  public void T_getPrimitiveArray_equalsSetValue() throws IOException {
    FixedNumEncoder encoder = new FixedNumEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    byte[] buffer = new byte[encoder.calcBinarySize(5)];
    long[] valueArray = new long[]{
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
    };
    boolean[] isNullArray = new boolean[]{
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
    encoder.toBinary( valueArray , buffer , 0 , 5 , ByteOrder.nativeOrder() );
    PrimitiveObject[] result = encoder.getPrimitiveArray( buffer , 0 , 5 , isNullArray , ByteOrder.nativeOrder() );
    for ( int i = 0 ; i < 10 ; i++ ) {
      if ( isNullArray[i] ) {
        assertNull( result[i] );
      } else {
        assertEquals( result[i].getLong() , valueArray[i/2] );
      }
    }
  }

  @Test
  public void T_setDictionary_equalsSetValue() throws IOException {
    FixedNumEncoder encoder = new FixedNumEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    byte[] buffer = new byte[encoder.calcBinarySize(10)];
    long[] valueArray = new long[]{
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE
    };
    encoder.toBinary( valueArray , buffer , 0 , 10 , ByteOrder.nativeOrder() );
    EncoderTestDictionary dic = new EncoderTestDictionary( 10 );
    encoder.setDictionary( buffer , 0 , 10 , ByteOrder.nativeOrder() , dic );
    long[] result = dic.getLongArray();
    for ( int i = 0 ; i < 10 ; i++ ) {
      assertEquals( result[i] , valueArray[i] );
    }
  }

  @Test
  public void T_loadInMemoryStorage_equalsSetValue() throws IOException {
    FixedNumEncoder encoder = new FixedNumEncoder( Long.MAX_VALUE , Long.MAX_VALUE );
    byte[] buffer = new byte[encoder.calcBinarySize(5)];
    long[] valueArray = new long[]{
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
      Long.MAX_VALUE,
    };
    boolean[] isNullArray = new boolean[]{
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
    encoder.toBinary( valueArray , buffer , 0 , 5 , ByteOrder.nativeOrder() );
    EncoderTestMemoryAllocator allocator = new EncoderTestMemoryAllocator( 10 );
    encoder.loadInMemoryStorage( buffer , 0 , 5 , isNullArray , ByteOrder.nativeOrder() , allocator , 0 );
    long[] result = allocator.getLongArray();
    boolean[] nullResult = allocator.getIsNullArray();
    for ( int i = 0 ; i < 10 ; i++ ) {
      if ( isNullArray[i] ) {
        assertTrue( nullResult[i] );
      } else {
        assertEquals( result[i] , valueArray[i/2] );
      }
    }
  }

}
