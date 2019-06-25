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

public class TestFloatObj {

  @Test
  public void T_get_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    Object result = obj.get();
    assertTrue( ( result instanceof Float ) );
    assertEquals( (float)1, ( ( (Float)result ).floatValue() ) );
  }

  @Test
  public void T_getString_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( "1.0" , obj.getString() );
  }

  @Test
  public void T_getBytes_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( "1.0" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( (float)1 , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withMaxValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Byte.MAX_VALUE ) );
    assertEquals( (float)Byte.MAX_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMaxValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Byte.MAX_VALUE + 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getByte_throwsException_withMinValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Byte.MIN_VALUE ) );
    assertEquals( (float)Byte.MIN_VALUE , obj.getByte() );
  }

  @Test
  public void T_getByte_throwsException_withGreaterThanMinValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Byte.MIN_VALUE - 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getShort_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( (float)1 , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withMaxValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Short.MAX_VALUE ) );
    assertEquals( (float)Short.MAX_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMaxValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Short.MAX_VALUE + 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getShort_throwsException_withMinValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Short.MIN_VALUE ) );
    assertEquals( (float)Short.MIN_VALUE , obj.getShort() );
  }

  @Test
  public void T_getShort_throwsException_withGreaterThanMinValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Short.MIN_VALUE - 1 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getInt_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withMaxValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Integer.MAX_VALUE ) );
    assertEquals( (float)Integer.MAX_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMaxValue() throws IOException {
    // Same as Integer.MAX_VALUE up to +128
    FloatObj obj = new FloatObj( (float)( (float)Integer.MAX_VALUE + (float)129 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getInt_throwsException_withMinValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Integer.MIN_VALUE ) );
    assertEquals( (float)Integer.MIN_VALUE , obj.getInt() );
  }

  @Test
  public void T_getInt_throwsException_withGreaterThanMinValue() throws IOException {
    // Same as Integer.MIN_VALUE up to -128
    FloatObj obj = new FloatObj( (float)( (float)Integer.MIN_VALUE - (float)129 ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getLong_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withMaxValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Long.MAX_VALUE ) );
    assertEquals( (float)Long.MAX_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMaxValue() throws IOException {
    // Same as Long.MAX_VALUE up to 549755846656
    FloatObj objMax = new FloatObj( (float)( (float)Long.MAX_VALUE + Long.valueOf( 549755846656L ).floatValue() ) );
    assertEquals( (float)Long.MAX_VALUE , objMax.getLong() );

    FloatObj obj = new FloatObj( (float)( (float)Long.MAX_VALUE + Long.valueOf( 549755846657L ).floatValue() ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getLong_throwsException_withMinValue() throws IOException {
    FloatObj obj = new FloatObj( (float)( Long.MIN_VALUE ) );
    assertEquals( (float)Long.MIN_VALUE , obj.getLong() );
  }

  @Test
  public void T_getLong_throwsException_withGreaterThanMinValue() throws IOException {
    // Same as Long.MIN_VALUE up to -549755846656
    FloatObj objMin = new FloatObj( (float)( (float)Long.MIN_VALUE - Long.valueOf( 549755846656L ).floatValue() ) );
    assertEquals( (float)Long.MIN_VALUE , objMin.getLong() );

    FloatObj obj = new FloatObj( (float)( (float)Long.MIN_VALUE - Long.valueOf( 549755846657L ).floatValue() ) );
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getFloat_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_equalsSetValue() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true() throws IOException {
    FloatObj obj = new FloatObj( (float)1 );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_false() throws IOException {
    FloatObj obj = new FloatObj( (float)0 );
    assertFalse( obj.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    FloatObj obj = new FloatObj( (float)0 );
    assertEquals( PrimitiveType.FLOAT , obj.getPrimitiveType() );
  }

}
