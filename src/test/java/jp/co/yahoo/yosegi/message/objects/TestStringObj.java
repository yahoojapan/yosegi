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

public class TestStringObj {

  @Test
  public void T_get_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "a" );
    Object result = obj.get();
    assertTrue( ( result instanceof String ) );
    assertEquals( "a" , result.toString() );
  }

  @Test
  public void T_getString_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "a" );
    assertEquals( "a" , obj.getString() );
  }

  @Test
  public void T_getBytes_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "a" );
    assertEquals( "a" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "1" );
    assertEquals( (byte)1 , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withMaxValue() throws IOException {
    StringObj obj = new StringObj( Byte.valueOf( Byte.MAX_VALUE ).toString() );
    assertEquals( Byte.MAX_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMaxValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( (long)Byte.MAX_VALUE + 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getByte_throwsException_withMinValue() throws IOException {
    StringObj obj = new StringObj( Byte.valueOf( Byte.MIN_VALUE ).toString() );
    assertEquals( (double)Byte.MIN_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMinValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( (long)Byte.MIN_VALUE - 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getShort_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "1" );
    assertEquals( (short)1 , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withMaxValue() throws IOException {
    StringObj obj = new StringObj( Short.valueOf( Short.MAX_VALUE ).toString() );
    assertEquals( Short.MAX_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMaxValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( (long)Short.MAX_VALUE + 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getShort_throwsException_withMinValue() throws IOException {
    StringObj obj = new StringObj( Short.valueOf( Short.MIN_VALUE ).toString() );
    assertEquals( Short.MIN_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMinValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( (long)Short.MIN_VALUE - 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getInt_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "1" );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withMaxValue() throws IOException {
    StringObj obj = new StringObj( Integer.valueOf( Integer.MAX_VALUE ).toString() );
    assertEquals( Integer.MAX_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMaxValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( (long)Integer.MAX_VALUE + 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getInt_throwsException_withMinValue() throws IOException {
    StringObj obj = new StringObj( Integer.valueOf( Integer.MIN_VALUE ).toString() );
    assertEquals( Integer.MIN_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMinValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( (long)Integer.MIN_VALUE - 1L ).toString() );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getLong_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "1" );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withMaxValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( Long.MAX_VALUE ).toString() );
    assertEquals( Long.MAX_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMaxValue() throws IOException {
    StringObj obj = new StringObj( "9223372036854775808" );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getLong_throwsException_withMinValue() throws IOException {
    StringObj obj = new StringObj( Long.valueOf( Long.MIN_VALUE ).toString() );
    assertEquals( Long.MIN_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMinValue() throws IOException {
    StringObj obj = new StringObj( "-9223372036854775809" );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getFloat_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "1" );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_equalsSetValue() throws IOException {
    StringObj obj = new StringObj( "1" );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true() throws IOException {
    StringObj obj = new StringObj( "true" );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_false() throws IOException {
    StringObj obj = new StringObj( "false" );
    assertFalse( obj.getBoolean() );
    StringObj obj2 = new StringObj( "a" );
    assertFalse( obj2.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    StringObj obj = new StringObj( "a" );
    assertEquals( PrimitiveType.STRING , obj.getPrimitiveType() );
  }

}
