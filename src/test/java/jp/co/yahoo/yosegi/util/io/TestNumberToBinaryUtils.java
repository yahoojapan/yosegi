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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestNumberToBinaryUtils {

  @Test
  public void T_getIntConverter_equalsSetValue_LessThanZero() throws IOException {
    NumberToBinaryUtils.IIntConverter converter = NumberToBinaryUtils.getIntConverter( -1 , 1 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.IntConverter4 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putInt( 0 );
    writer.putInt( 1 );
    writer.putInt( -1 );
    writer.putInt( 0 );
    writer.putInt( 1 );
    writer.putInt( -1 );
    writer.putInt( 0 );
    writer.putInt( 1 );
    writer.putInt( -1 );
    writer.putInt( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 1 , reader.getInt() );
    assertEquals( -1 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 1 , reader.getInt() );
    assertEquals( -1 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 1 , reader.getInt() );
    assertEquals( -1 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
  }

  @Test
  public void T_getIntConverter_equalsSetValue_highestBit25() throws IOException {
    NumberToBinaryUtils.IIntConverter tmpConverter = NumberToBinaryUtils.getIntConverter( 0 , 16777215 );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.IntConverter3 ) );
    NumberToBinaryUtils.IIntConverter converter = NumberToBinaryUtils.getIntConverter( 0 , 16777216 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.IntConverter4 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putInt( 0 );
    writer.putInt( Integer.MAX_VALUE );
    writer.putInt( Integer.MIN_VALUE );
    writer.putInt( 0 );
    writer.putInt( Integer.MAX_VALUE );
    writer.putInt( Integer.MIN_VALUE );
    writer.putInt( 0 );
    writer.putInt( Integer.MAX_VALUE );
    writer.putInt( Integer.MIN_VALUE );
    writer.putInt( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getInt() );
    assertEquals( Integer.MAX_VALUE , reader.getInt() );
    assertEquals( Integer.MIN_VALUE , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( Integer.MAX_VALUE , reader.getInt() );
    assertEquals( Integer.MIN_VALUE , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( Integer.MAX_VALUE , reader.getInt() );
    assertEquals( Integer.MIN_VALUE , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
  }

  @Test
  public void T_getIntConverter_equalsSetValue_highestBit17() throws IOException {
    NumberToBinaryUtils.IIntConverter tmpConverter = NumberToBinaryUtils.getIntConverter( 0 , 65535 );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.IntConverter2 ) );
    NumberToBinaryUtils.IIntConverter converter = NumberToBinaryUtils.getIntConverter( 0 , 65536 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.IntConverter3 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putInt( 0 );
    writer.putInt( 16777215 );
    writer.putInt( 65535 );
    writer.putInt( 0 );
    writer.putInt( 16777215 );
    writer.putInt( 65535 );
    writer.putInt( 0 );
    writer.putInt( 16777215 );
    writer.putInt( 65535 );
    writer.putInt( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 16777215 , reader.getInt() );
    assertEquals( 65535 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 16777215 , reader.getInt() );
    assertEquals( 65535 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 16777215 , reader.getInt() );
    assertEquals( 65535 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
  }

  @Test
  public void T_getIntConverter_equalsSetValue_highestBit9() throws IOException {
    NumberToBinaryUtils.IIntConverter tmpConverter = NumberToBinaryUtils.getIntConverter( 0 , 255 );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.IntConverter1 ) );
    NumberToBinaryUtils.IIntConverter converter = NumberToBinaryUtils.getIntConverter( 0 , 256 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.IntConverter2 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putInt( 0 );
    writer.putInt( 65535 );
    writer.putInt( 255 );
    writer.putInt( 0 );
    writer.putInt( 65535 );
    writer.putInt( 255 );
    writer.putInt( 0 );
    writer.putInt( 65535 );
    writer.putInt( 255 );
    writer.putInt( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 65535 , reader.getInt() );
    assertEquals( 255 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 65535 , reader.getInt() );
    assertEquals( 255 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 65535 , reader.getInt() );
    assertEquals( 255 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
  }

  @Test
  public void T_getIntConverter_equalsSetValue_highestBit1() throws IOException {
    NumberToBinaryUtils.IIntConverter tmpConverter = NumberToBinaryUtils.getIntConverter( 0 , 0 );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.IntConverter0 ) );
    NumberToBinaryUtils.IIntConverter converter = NumberToBinaryUtils.getIntConverter( 0 , 1 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.IntConverter1 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putInt( 0 );
    writer.putInt( 1 );
    writer.putInt( 255 );
    writer.putInt( 0 );
    writer.putInt( 1 );
    writer.putInt( 255 );
    writer.putInt( 0 );
    writer.putInt( 1 );
    writer.putInt( 255 );
    writer.putInt( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 1 , reader.getInt() );
    assertEquals( 255 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 1 , reader.getInt() );
    assertEquals( 255 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 1 , reader.getInt() );
    assertEquals( 255 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
  }

  @Test
  public void T_getIntConverter_equalsSetValue_highestBit0() throws IOException {
    NumberToBinaryUtils.IIntConverter converter = NumberToBinaryUtils.getIntConverter( 0 , 0 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.IntConverter0 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putInt( 0 );
    writer.putInt( 0 );
    writer.putInt( 0);
    writer.putInt( 0 );
    writer.putInt( 0 );
    writer.putInt( 0 );
    writer.putInt( 0 );
    writer.putInt( 0 );
    writer.putInt( 0 );
    writer.putInt( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
    assertEquals( 0 , reader.getInt() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_LessThanZero() throws IOException {
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( -1 , 1 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter8 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( 1 );
    writer.putLong( -1 );
    writer.putLong( 0 );
    writer.putLong( 1 );
    writer.putLong( -1 );
    writer.putLong( 0 );
    writer.putLong( 1 );
    writer.putLong( -1 );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 1 , reader.getLong() );
    assertEquals( -1 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 1 , reader.getLong() );
    assertEquals( -1 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 1 , reader.getLong() );
    assertEquals( -1 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit49() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 48 ) -1L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter6 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 48 ) );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter7 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 56 ) -1L );
    writer.putLong( ( 1L << 40 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 56 ) -1L );
    writer.putLong( ( 1L << 40 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 56 ) -1L );
    writer.putLong( ( 1L << 40 ) );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 56 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 40 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 56 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 40 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 56 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 40 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit41() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 40 ) -1L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter5 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 40 ) );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter6 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 48 ) -1L );
    writer.putLong( ( 1L << 32 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 48 ) -1L );
    writer.putLong( ( 1L << 32 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 48 ) -1L );
    writer.putLong( ( 1L << 32 ) );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 48 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 32 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 48 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 32 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 48 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 32 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit33() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 32 ) -1L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter4 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 32 ) );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter5 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 40 ) -1L );
    writer.putLong( ( 1L << 24 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 40 ) -1L );
    writer.putLong( ( 1L << 24 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 40 ) -1L );
    writer.putLong( ( 1L << 24 ) );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 40 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 24 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 40 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 24 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 40 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 24 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit25() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 24 ) -1L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter3 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 24 ) );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter4 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 32 ) -1L );
    writer.putLong( ( 1L << 16 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 32 ) -1L );
    writer.putLong( ( 1L << 16 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 32 ) -1L );
    writer.putLong( ( 1L << 16 ) );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 32 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 16 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 32 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 16 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 32 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 16 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit17() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 16 ) -1L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter2 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 16 ) );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter3 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 24 ) -1L );
    writer.putLong( ( 1L << 8 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 24 ) -1L );
    writer.putLong( ( 1L << 8 ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 24 ) -1L );
    writer.putLong( ( 1L << 8 ) );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 24 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 8 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 24 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 8 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 24 ) -1L , reader.getLong() );
    assertEquals( ( 1L << 8 ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit9() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 8 ) -1L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter1 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , ( 1L << 8 ) );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter2 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 16 ) -1L );
    writer.putLong( ( 1L ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 16 ) -1L );
    writer.putLong( ( 1L ) );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 16 ) -1L );
    writer.putLong( ( 1L ) );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 16 ) -1L , reader.getLong() );
    assertEquals( ( 1L ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 16 ) -1L , reader.getLong() );
    assertEquals( ( 1L ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 16 ) -1L , reader.getLong() );
    assertEquals( ( 1L ) , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit1() throws IOException {
    NumberToBinaryUtils.ILongConverter tmpConverter = NumberToBinaryUtils.getLongConverter( 0 , 0L );
    assertTrue( ( tmpConverter instanceof NumberToBinaryUtils.LongConverter0 ) );
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , 1L );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter1 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 8 ) -1L );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 8 ) -1L );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( ( 1L << 8 ) -1L );
    writer.putLong( 0 );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 8 ) -1L , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 8 ) -1L , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( ( 1L << 8 ) -1L , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

  @Test
  public void T_getLongConverter_equalsSetValue_highestBit0() throws IOException {
    NumberToBinaryUtils.ILongConverter converter = NumberToBinaryUtils.getLongConverter( 0 , 0 );
    assertTrue( ( converter instanceof NumberToBinaryUtils.LongConverter0 ) );
    byte[] buffer = new byte[ converter.calcBinarySize( 10 ) ];
    IWriteSupporter writer = converter.toWriteSuppoter( 10 , buffer , 0 , buffer.length );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );
    writer.putLong( 0 );

    IReadSupporter reader = converter.toReadSupporter( buffer , 0 , buffer.length );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
    assertEquals( 0 , reader.getLong() );
  }

}
