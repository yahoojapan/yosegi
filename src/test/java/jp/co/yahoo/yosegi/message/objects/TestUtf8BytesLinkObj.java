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

package jp.co.yahoo.yosegi.message.objects;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestUtf8BytesLinkObj {

  @Test
  public void T_get_equalsSetValue() throws IOException {
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( "a".getBytes() , 0 , 1 );
    assertThrows( IOException.class ,
      () -> {
        obj.get();
      }
    );
  }

  @Test
  public void T_getString_equalsSetValue() throws IOException {
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( "a".getBytes() , 0 , 1 );
    assertEquals( "a" , obj.getString() );
  }

  @Test
  public void T_getBytes_equalsSetValue() throws IOException {
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( "a".getBytes() , 0 , 1 );
    assertEquals( "a" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_equalsSetValue() throws IOException {
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( "1".getBytes() , 0 , 1 );
    assertEquals( (byte)1 , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withMaxValue() throws IOException {
    byte[] b = Byte.valueOf( Byte.MAX_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Byte.MAX_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMaxValue() throws IOException {
    byte[] b = Long.valueOf( (long)Byte.MAX_VALUE + 1L ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getByte_throwsException_withMinValue() throws IOException {
    byte[] b = Byte.valueOf( Byte.MIN_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( (double)Byte.MIN_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMinValue() throws IOException {
    byte[] b = Long.valueOf( (long)Byte.MIN_VALUE - 1L ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getShort_equalsSetValue() throws IOException {
    byte[] b = "1".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( (short)1 , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withMaxValue() throws IOException {
    byte[] b = Short.valueOf( Short.MAX_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Short.MAX_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMaxValue() throws IOException {
    byte[] b = Long.valueOf( (long)Short.MAX_VALUE + 1L ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getShort_throwsException_withMinValue() throws IOException {
    byte[] b = Short.valueOf( Short.MIN_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Short.MIN_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMinValue() throws IOException {
    byte[] b = Long.valueOf( (long)Short.MIN_VALUE - 1L ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getInt_equalsSetValue() throws IOException {
    byte[] b = "1".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withMaxValue() throws IOException {
    byte[] b = Integer.valueOf( Integer.MAX_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Integer.MAX_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMaxValue() throws IOException {
    byte[] b = Long.valueOf( (long)Integer.MAX_VALUE + 1L ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getInt_throwsException_withMinValue() throws IOException {
    byte[] b = Integer.valueOf( Integer.MIN_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Integer.MIN_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMinValue() throws IOException {
    byte[] b = Long.valueOf( (long)Integer.MIN_VALUE - 1L ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getLong_equalsSetValue() throws IOException {
    byte[] b = "1".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withMaxValue() throws IOException {
    byte[] b = Long.valueOf( Long.MAX_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Long.MAX_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMaxValue() throws IOException {
    byte[] b = "9223372036854775808".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getLong_throwsException_withMinValue() throws IOException {
    byte[] b = Long.valueOf( Long.MIN_VALUE ).toString().getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( Long.MIN_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMinValue() throws IOException {
    byte[] b = "-9223372036854775809".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getFloat_equalsSetValue() throws IOException {
    byte[] b = "1".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_equalsSetValue() throws IOException {
    byte[] b = "1".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true() throws IOException {
    byte[] b = "true".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_false() throws IOException {
    byte[] b = "false".getBytes();
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( b , 0 , b.length );
    assertFalse( obj.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    Utf8BytesLinkObj obj = new Utf8BytesLinkObj( "a".getBytes() , 0 , 1 );
    assertEquals( PrimitiveType.STRING , obj.getPrimitiveType() );
  }

}
