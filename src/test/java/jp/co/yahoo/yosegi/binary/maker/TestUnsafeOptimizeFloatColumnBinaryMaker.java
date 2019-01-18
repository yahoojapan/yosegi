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
package jp.co.yahoo.yosegi.binary.maker;

import java.io.IOException;
import java.nio.*;

import java.util.List;
import java.util.ArrayList;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

public class TestUnsafeOptimizeFloatColumnBinaryMaker{

  @Test
  public void T_chooseDictionaryIndexMaker_1(){
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( -1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( 0 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( 128 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH - 1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertFalse( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ByteDictionaryIndexMaker );
  }

  @Test
  public void T_chooseDictionaryIndexMaker_2(){
    assertFalse( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( 30000 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH - 1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertFalse( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.ShortDictionaryIndexMaker );
  }

  @Test
  public void T_chooseDictionaryIndexMaker_3(){
    assertFalse( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.IntDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.IntDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( Integer.MAX_VALUE ) instanceof UnsafeOptimizeFloatColumnBinaryMaker.IntDictionaryIndexMaker );
  }

  @Test
  public void T_chooseDictionaryIndexMaker_Byte_1() throws IOException{
    UnsafeOptimizeFloatColumnBinaryMaker.IDictionaryIndexMaker indexMaker = UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( 127 );
    assertEquals( ( Byte.BYTES * -1 ) , indexMaker.calcBinarySize( -1 ) );
    assertEquals( ( Byte.BYTES * 0 ) , indexMaker.calcBinarySize( 0 ) );
    assertEquals( ( Byte.BYTES * 256 ) , indexMaker.calcBinarySize( 256 ) );

    int[] dicIndex = new int[128];
    for( int i = 0,n = 0 ; i < dicIndex.length ; i++,n+=2 ){
      dicIndex[i] = n;
    }
    byte[] b = new byte[indexMaker.calcBinarySize( dicIndex.length ) ];
    indexMaker.create( dicIndex , b , 0 , b.length , ByteOrder.nativeOrder() );
    IntBuffer intBuffer = indexMaker.getIndexIntBuffer( b , 0 , b.length , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < intBuffer.capacity() ; i++ ){
      assertEquals( dicIndex[i] , intBuffer.get() );
    }
  }

  @Test
  public void T_chooseDictionaryIndexMaker_Short_1() throws IOException{
    UnsafeOptimizeFloatColumnBinaryMaker.IDictionaryIndexMaker indexMaker = UnsafeOptimizeFloatColumnBinaryMaker.chooseDictionaryIndexMaker( 0xFFFF );
    assertEquals( ( Short.BYTES * -1 ) , indexMaker.calcBinarySize( -1 ) );
    assertEquals( ( Short.BYTES * 0 ) , indexMaker.calcBinarySize( 0 ) );
    assertEquals( ( Short.BYTES * 256 ) , indexMaker.calcBinarySize( 256 ) );

    int[] dicIndex = new int[128];
    for( int i = 0,n = ( 0xFFFF - 256 ) ; i < dicIndex.length ; i++,n+=2 ){
      dicIndex[i] = n;
    }
    byte[] b = new byte[indexMaker.calcBinarySize( dicIndex.length ) ];
    indexMaker.create( dicIndex , b , 0 , b.length , ByteOrder.nativeOrder() );
    IntBuffer intBuffer = indexMaker.getIndexIntBuffer( b , 0 , b.length , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < intBuffer.capacity() ; i++ ){
      assertEquals( dicIndex[i] , intBuffer.get() );
    }
  }

}
