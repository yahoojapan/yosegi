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
import java.nio.ByteOrder;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

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
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.blockindex.LongRangeBlockIndex;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.util.io.IWriteSupporter;
import jp.co.yahoo.yosegi.util.io.IReadSupporter;
import jp.co.yahoo.yosegi.util.io.NumberToBinaryUtils;
import jp.co.yahoo.yosegi.util.io.unsafe.ByteBufferSupporterFactory;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestUnsafeOptimizeDumpLongColumnBinaryMaker{

  private final Random rnd = new Random();

  @Test
  public void T_getDiffColumnType_1(){
    long min = 0;
    long max = -100;

    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( min , max ) , ColumnType.LONG );
  }

  @Test
  public void T_getDiffColumnType_2(){
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFF ) , ColumnType.BYTE );
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( -255 , 0 ) , ColumnType.BYTE );

    assertNotEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFF + 1 ) , ColumnType.BYTE );
    assertNotEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( -256 , 0 ) , ColumnType.BYTE );
  }

  @Test
  public void T_getDiffColumnType_3(){
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFF ) , ColumnType.SHORT );
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( -65535 , 0 ) , ColumnType.SHORT );

    assertNotEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFF + 1 ) , ColumnType.SHORT );
    assertNotEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( -65536  , 0 ) , ColumnType.SHORT );
  }

  @Test
  public void T_getDiffColumnType_4(){
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFFFFFFL ) , ColumnType.INTEGER );
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( -4294967295L , 0 ) , ColumnType.INTEGER );

    assertNotEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( 0 , 0xFFFFFFFFL + 1 ) , ColumnType.INTEGER );
    assertNotEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getDiffColumnType( -4294967296L  , 0 ) , ColumnType.INTEGER );

  }

  @Test
  public void T_getLogicalSize_1(){
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getLogicalSize( 100 , ColumnType.BYTE ) , Byte.BYTES * 100 );
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getLogicalSize( 100 , ColumnType.SHORT ) , Short.BYTES * 100 );
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getLogicalSize( 100 , ColumnType.INTEGER ) , Integer.BYTES * 100 );
    assertEquals( UnsafeOptimizeDumpLongColumnBinaryMaker.getLogicalSize( 100 , ColumnType.LONG ) , Long.BYTES * 100 );
  }

  @Test
  public void T_BinaryMaker_byte_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Byte.valueOf( Byte.MIN_VALUE ).longValue() , Byte.valueOf( Byte.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.ByteBinaryMaker );

    long[] l = new long[100];
    l[0] = 0;
    l[1] = Byte.valueOf( Byte.MIN_VALUE ).longValue();
    l[2] = Byte.valueOf( Byte.MAX_VALUE ).longValue();

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = rnd.nextLong() % Byte.valueOf( Byte.MIN_VALUE ).longValue();
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    assertEquals( b.length , Byte.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

  @Test
  public void T_BinaryMaker_diffByte_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Long.MIN_VALUE , Long.MIN_VALUE + 0xFFL );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.DiffByteBinaryMaker );

    long[] l = new long[100];
    l[0] = Long.MIN_VALUE + 0xFFL;
    l[1] = Long.MIN_VALUE + 128;
    l[2] = Long.MIN_VALUE;

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = Long.MIN_VALUE + ( rnd.nextInt( 0xFF ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    assertEquals( b.length , Byte.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

  @Test
  public void T_BinaryMaker_short_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Short.valueOf( Short.MIN_VALUE ).longValue() , Short.valueOf( Short.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.ShortBinaryMaker );

    long[] l = new long[100];
    l[0] = 0;
    l[1] = Short.valueOf( Short.MIN_VALUE ).longValue();
    l[2] = Short.valueOf( Short.MAX_VALUE ).longValue();

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = rnd.nextLong() % Short.valueOf( Short.MIN_VALUE ).longValue();
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    assertEquals( b.length , Short.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

  @Test
  public void T_BinaryMaker_diffShort_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Long.MIN_VALUE , Long.MIN_VALUE + 0xFFFFL );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.DiffShortBinaryMaker );

    long[] l = new long[100];
    l[0] = Long.MIN_VALUE + 0xFFFFL;
    l[1] = Long.MIN_VALUE + 65535;
    l[2] = Long.MIN_VALUE;

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = Long.MIN_VALUE + ( rnd.nextInt( 0xFFFF ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    assertEquals( b.length , Short.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

  @Test
  public void T_BinaryMaker_int_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Integer.valueOf( Integer.MIN_VALUE ).longValue() , Integer.valueOf( Integer.MAX_VALUE ).longValue() );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.IntBinaryMaker );

    long[] l = new long[100];
    l[0] = 0;
    l[1] = Integer.valueOf( Integer.MIN_VALUE ).longValue();
    l[2] = Integer.valueOf( Integer.MAX_VALUE ).longValue();

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = rnd.nextLong() % Integer.valueOf( Integer.MIN_VALUE ).longValue();
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    // not equal Integer * 100
    //assertEquals( b.length , Integer.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

  @Test
  public void T_BinaryMaker_diffInt_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Long.MIN_VALUE , Long.MIN_VALUE + 0xFFFFFFFFL );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.DiffIntBinaryMaker );

    long[] l = new long[100];
    l[0] = Long.MIN_VALUE + 0xFFFFFFFFL;
    l[1] = Long.MIN_VALUE + 4294967295L;
    l[2] = Long.MIN_VALUE;

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = Long.MIN_VALUE + ( rnd.nextInt( Integer.MAX_VALUE ) );
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    // not equal Integer * 100
    //assertEquals( b.length , Integer.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

  @Test
  public void T_BinaryMaker_long_1() throws IOException{
    UnsafeOptimizeDumpLongColumnBinaryMaker.IBinaryMaker maker = UnsafeOptimizeDumpLongColumnBinaryMaker.chooseBinaryMaker( Long.MIN_VALUE , Long.MAX_VALUE );
    assertTrue( maker instanceof UnsafeOptimizeDumpLongColumnBinaryMaker.LongBinaryMaker );

    long[] l = new long[100];
    l[0] = 0;
    l[1] = Long.MIN_VALUE;
    l[2] = Long.MAX_VALUE;

    for( int i = 3 ; i < 100 ; i++ ){
      l[i] = rnd.nextLong();
    }
    byte[] b = new byte[ maker.calcBinarySize( l.length ) ];
    // not equal Long * 100
    //assertEquals( b.length , Long.BYTES * 100 );
    maker.create( l , b , 0 , b.length , ByteOrder.nativeOrder() , l.length );

    PrimitiveObject[] rp = maker.getPrimitiveArray( b , 0 , b.length , l.length , false , ByteOrder.nativeOrder() );
    for( int i = 0 ; i < rp.length ; i++ ){
      assertEquals( rp[i].getLong() , l[i] );
    }
  }

}
