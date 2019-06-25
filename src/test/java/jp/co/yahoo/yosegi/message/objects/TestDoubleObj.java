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

public class TestDoubleObj {

  @Test
  public void T_get_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    Object result = obj.get();
    assertTrue( ( result instanceof Double ) );
    assertEquals( (double)1, ( ( (Double)result ).doubleValue() ) );
  }

  @Test
  public void T_getString_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( "1.0" , obj.getString() );
  }

  @Test
  public void T_getBytes_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( "1.0" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( (double)1 , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Byte.MAX_VALUE ) );
    assertEquals( (double)Byte.MAX_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Byte.MAX_VALUE + 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getByte_throwsException_withMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Byte.MIN_VALUE ) );
    assertEquals( (double)Byte.MIN_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Byte.MIN_VALUE - 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getShort_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( (double)1 , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Short.MAX_VALUE ) );
    assertEquals( (double)Short.MAX_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Short.MAX_VALUE + 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getShort_throwsException_withMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Short.MIN_VALUE ) );
    assertEquals( (double)Short.MIN_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Short.MIN_VALUE - 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getInt_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Integer.MAX_VALUE ) );
    assertEquals( (double)Integer.MAX_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( (double)Integer.MAX_VALUE + (double)1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getInt_throwsException_withMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Integer.MIN_VALUE ) );
    assertEquals( (double)Integer.MIN_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( (double)Integer.MIN_VALUE - (double)1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getLong_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withMaxValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Long.MAX_VALUE ) );
    assertEquals( (double)Long.MAX_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMaxValue() throws IOException {
    // Same as Long.MAX_VALUE up to 1024
    DoubleObj objMax = new DoubleObj( (double)( (double)Long.MAX_VALUE + Long.valueOf( 1024L ).doubleValue() ) );
    assertEquals( (double)Long.MAX_VALUE , objMax.getLong() );

    DoubleObj obj = new DoubleObj( (double)( (double)Long.MAX_VALUE + Long.valueOf( 1025L ).doubleValue() ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getLong_throwsException_withMinValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)( Long.MIN_VALUE ) );
    assertEquals( (double)Long.MIN_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMinValue() throws IOException {
    // Same as Long.MIN_VALUE up to 1024
    DoubleObj objMin = new DoubleObj( (double)( (double)Long.MIN_VALUE - Long.valueOf( 1024L ).doubleValue() ) );
    assertEquals( (double)Long.MIN_VALUE , objMin.getLong() );

    DoubleObj obj = new DoubleObj( (double)( (double)Long.MIN_VALUE - Long.valueOf( 1025L ).doubleValue() ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getFloat_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_equalsSetValue() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true() throws IOException {
    DoubleObj obj = new DoubleObj( (double)1 );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_false() throws IOException {
    DoubleObj obj = new DoubleObj( (double)0 );
    assertFalse( obj.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    DoubleObj obj = new DoubleObj( (double)0 );
    assertEquals( PrimitiveType.DOUBLE , obj.getPrimitiveType() );
  }

}
