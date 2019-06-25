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

public class TestBytesObj {

  @Test
  public void T_get_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "a".getBytes() );
    Object result = obj.get();
    assertTrue( ( result instanceof byte[] ) );
    assertEquals( "a" , new String( (byte[])result ) );
  }

  @Test
  public void T_getString_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "a".getBytes() );
    assertEquals( "a" , obj.getString() );
  }

  @Test
  public void T_getBytes_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "a".getBytes() );
    assertEquals( "a" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "1".getBytes() );
    assertEquals( (byte)1 , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Byte.valueOf( Byte.MAX_VALUE ).toString().getBytes() );
    assertEquals( Byte.MAX_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( (long)Byte.MAX_VALUE + 1L ).toString().getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getByte_throwsException_withMinValue() throws IOException {
    BytesObj obj = new BytesObj( Byte.valueOf( Byte.MIN_VALUE ).toString().getBytes() );
    assertEquals( (double)Byte.MIN_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMinValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( (long)Byte.MIN_VALUE - 1L ).toString().getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getShort_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "1".getBytes() );
    assertEquals( (short)1 , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Short.valueOf( Short.MAX_VALUE ).toString().getBytes() );
    assertEquals( Short.MAX_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( (long)Short.MAX_VALUE + 1L ).toString().getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getShort_throwsException_withMinValue() throws IOException {
    BytesObj obj = new BytesObj( Short.valueOf( Short.MIN_VALUE ).toString().getBytes() );
    assertEquals( Short.MIN_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMinValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( (long)Short.MIN_VALUE - 1L ).toString().getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getInt_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "1".getBytes() );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Integer.valueOf( Integer.MAX_VALUE ).toString().getBytes() );
    assertEquals( Integer.MAX_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( (long)Integer.MAX_VALUE + 1L ).toString().getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getInt_throwsException_withMinValue() throws IOException {
    BytesObj obj = new BytesObj( Integer.valueOf( Integer.MIN_VALUE ).toString().getBytes() );
    assertEquals( Integer.MIN_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMinValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( (long)Integer.MIN_VALUE - 1L ).toString().getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getLong_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "1".getBytes() );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withMaxValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( Long.MAX_VALUE ).toString().getBytes() );
    assertEquals( Long.MAX_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMaxValue() throws IOException {
    BytesObj obj = new BytesObj( "9223372036854775808".getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getLong_throwsException_withMinValue() throws IOException {
    BytesObj obj = new BytesObj( Long.valueOf( Long.MIN_VALUE ).toString().getBytes() );
    assertEquals( Long.MIN_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMinValue() throws IOException {
    BytesObj obj = new BytesObj( "-9223372036854775809".getBytes() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getFloat_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "1".getBytes() );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_equalsSetValue() throws IOException {
    BytesObj obj = new BytesObj( "1".getBytes() );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true() throws IOException {
    BytesObj obj = new BytesObj( "true".getBytes() );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_false() throws IOException {
    BytesObj obj = new BytesObj( "false".getBytes() );
    assertFalse( obj.getBoolean() );
    BytesObj obj2 = new BytesObj( "a".getBytes() );
    assertFalse( obj2.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    BytesObj obj = new BytesObj( "a".getBytes() );
    assertEquals( PrimitiveType.BYTES , obj.getPrimitiveType() );
  }

}
