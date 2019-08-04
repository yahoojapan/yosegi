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

public class TestByteObj {

  @Test
  public void T_get_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    Object result = obj.get();
    assertTrue( ( result instanceof Byte ) );
    assertEquals( (byte)1 , ( ( (Byte)result ).byteValue() ) );
  }

  @Test
  public void T_getString_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( "1" , obj.getString() );
  }

  @Test
  public void T_getBytes_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( "1" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( (byte)1 , obj.getByte() );
  }

  @Test
  public void T_getShort_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( (short)1 , obj.getShort() );
  }

  @Test
  public void T_getInt_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getLong_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getFloat_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_equalsSetValue() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true() throws IOException {
    ByteObj obj = new ByteObj( (byte)1 );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_false() throws IOException {
    ByteObj obj = new ByteObj( (byte)0 );
    assertFalse( obj.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    ByteObj obj = new ByteObj( (byte)0 );
    assertEquals( PrimitiveType.BYTE , obj.getPrimitiveType() );
  }

}
