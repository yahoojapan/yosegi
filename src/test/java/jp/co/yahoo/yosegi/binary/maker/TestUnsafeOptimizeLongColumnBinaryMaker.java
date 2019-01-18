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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ByteOrder;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;


import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;

import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.ByteColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.ShortColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.IntegerColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.analyzer.LongColumnAnalizeResult;
import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.maker.index.RangeLongIndex;
import jp.co.yahoo.yosegi.binary.maker.index.BufferDirectSequentialNumberCellIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;

public class TestUnsafeOptimizeLongColumnBinaryMaker{

  private final Random rnd = new Random();

  @Test
  public void T_getDiffColumnType_1(){
    long min = 0;
    long max = -100;

    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( min , max ) , ColumnType.LONG );
  }

  @Test
  public void T_getDiffColumnType_2(){
    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFF ) , ColumnType.BYTE );
    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( -255 , 0 ) , ColumnType.BYTE );

    assertNotEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFF + 1 ) , ColumnType.BYTE );
    assertNotEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( -256 , 0 ) , ColumnType.BYTE );
  }

  @Test
  public void T_getDiffColumnType_3(){
    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFF ) , ColumnType.SHORT );
    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( -65535 , 0 ) , ColumnType.SHORT );

    assertNotEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFF + 1 ) , ColumnType.SHORT );
    assertNotEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( -65536  , 0 ) , ColumnType.SHORT );
  }

  @Test
  public void T_getDiffColumnType_4(){
    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFFFFFFL ) , ColumnType.INTEGER );
    assertEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( -4294967295L , 0 ) , ColumnType.INTEGER );

    assertNotEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFFFFFFL + 1 ) , ColumnType.INTEGER );
    assertNotEquals( UnsafeOptimizeLongColumnBinaryMaker.getDiffColumnType( -4294967296L  , 0 ) , ColumnType.INTEGER );

  }

  @Test
  public void T_BinaryMaker_byte_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Byte.valueOf( Byte.MIN_VALUE ).longValue() , Byte.valueOf( Byte.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Byte.valueOf( Byte.MIN_VALUE ).longValue() ) );
    l.add( new LongObj( Byte.valueOf( Byte.MAX_VALUE ).longValue() ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( rnd.nextLong() % Byte.valueOf( Byte.MIN_VALUE ).longValue() ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    assertEquals( b.length , Byte.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_diffByte_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Long.MIN_VALUE , Long.MIN_VALUE + 0xFFL );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.DiffByteDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Long.MIN_VALUE + 0xFFL ) );
    l.add( new LongObj( Long.MIN_VALUE + 128  ) );
    l.add( new LongObj( Long.MIN_VALUE ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( Long.MIN_VALUE + ( rnd.nextInt( 0xFF ) ) ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    assertEquals( b.length , Byte.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_short_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Short.valueOf( Short.MIN_VALUE ).longValue() , Short.valueOf( Short.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Short.valueOf( Short.MIN_VALUE ).longValue() ) );
    l.add( new LongObj( Short.valueOf( Short.MAX_VALUE ).longValue() ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( rnd.nextLong() % Short.valueOf( Short.MIN_VALUE ).longValue() ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    assertEquals( b.length , Short.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_diffShort_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Long.MIN_VALUE , Long.MIN_VALUE + 0xFFFFL );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.DiffShortDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Long.MIN_VALUE + 0xFFFFL ) );
    l.add( new LongObj( Long.MIN_VALUE + 65535 ) );
    l.add( new LongObj( Long.MIN_VALUE ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( Long.MIN_VALUE + ( rnd.nextInt( 0xFFFF ) ) ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    assertEquals( b.length , Short.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_int_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Integer.valueOf( Integer.MIN_VALUE ).longValue() , Integer.valueOf( Integer.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.IntDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Integer.valueOf( Integer.MIN_VALUE ).longValue() ) );
    l.add( new LongObj( Integer.valueOf( Integer.MAX_VALUE ).longValue() ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( rnd.nextLong() % Integer.valueOf( Integer.MIN_VALUE ).longValue() ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_diffInt_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Long.MIN_VALUE , Long.MIN_VALUE + 0xFFFFFFFFL );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.DiffIntDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Long.MIN_VALUE + 0xFFFFFFFFL ) );
    l.add( new LongObj( Long.MIN_VALUE + 4294967295L ) );
    l.add( new LongObj( Long.MIN_VALUE ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( Long.MIN_VALUE + ( rnd.nextInt( Integer.MAX_VALUE ) ) ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_diffInt_2() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( 1545879688722L , 1545881797492L );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.DiffIntDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(2);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( 1545881701856L ) );
    l.add( new LongObj( 1545879728989L ) );
    l.add( new LongObj( 1545879882906L ) );
    l.add( new LongObj( 1545879688722L ) );

    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_BinaryMaker_long_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryMaker maker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryMaker( Long.valueOf( Long.MIN_VALUE ).longValue() , Long.valueOf( Long.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeLongColumnBinaryMaker.LongDictionaryMaker );

    List<PrimitiveObject> l = new ArrayList<PrimitiveObject>(100);
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( 0 ) );
    l.add( new LongObj( Long.MIN_VALUE ) );
    l.add( new LongObj( Long.MAX_VALUE ) );

    for( int i = 4 ; i < 100 ; i++ ){
      l.add( new LongObj( rnd.nextLong() ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.size() ) ];
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() );

    PrimitiveObject[] rp = maker.getDicPrimitiveArray( l.size() , b , 0 , b.length , ByteOrder.nativeOrder() );
    assertEquals( rp.length , l.size() );
    assertNull( rp[0] );
    for( int i = 1 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l.get( i ).getLong() );
    }
  }

  @Test
  public void T_chooseDictionaryIndexMaker_1(){
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( -1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( 0 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( 128 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH - 1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryIndexMaker );
    assertFalse( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ByteDictionaryIndexMaker );
  }

  @Test
  public void T_chooseDictionaryIndexMaker_2(){
    assertFalse( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_BYTE_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( 30000 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH - 1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryIndexMaker );
    assertFalse( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.ShortDictionaryIndexMaker );
  }

  @Test
  public void T_chooseDictionaryIndexMaker_3(){
    assertFalse( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH ) instanceof UnsafeOptimizeLongColumnBinaryMaker.IntDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( NumberToBinaryUtils.INT_SHORT_MAX_LENGTH + 1 ) instanceof UnsafeOptimizeLongColumnBinaryMaker.IntDictionaryIndexMaker );
    assertTrue( UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( Integer.MAX_VALUE ) instanceof UnsafeOptimizeLongColumnBinaryMaker.IntDictionaryIndexMaker );
  }

  @Test
  public void T_chooseDictionaryIndexMaker_Byte_1() throws IOException{
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryIndexMaker indexMaker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( 127 );
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
    UnsafeOptimizeLongColumnBinaryMaker.IDictionaryIndexMaker indexMaker = UnsafeOptimizeLongColumnBinaryMaker.chooseDictionaryIndexMaker( 0xFFFF );
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
