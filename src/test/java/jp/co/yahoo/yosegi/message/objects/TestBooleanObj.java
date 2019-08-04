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

public class TestBooleanObj {

  @Test
  public void T_get_booleanTrue_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    Object result = obj.get();
    assertTrue( ( result instanceof Boolean ) );
    assertTrue( ( ( (Boolean)result ).booleanValue() ) );
  }

  @Test
  public void T_get_booleanTrue_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    Object result = obj.get();
    assertTrue( ( result instanceof Boolean ) );
    assertFalse( ( ( (Boolean)result ).booleanValue() ) );
  }

  @Test
  public void T_getString_trueString_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( "true" , obj.getString() );
  }

  @Test
  public void T_getString_falseString_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( "false" , obj.getString() );
  }

  @Test
  public void T_getBytes_trueBytes_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( "true" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getBytes_falseBytes_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( "false" , new String( obj.getBytes() ) );
  }

  @Test
  public void T_getByte_trueNum_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( (byte)1 , obj.getByte() );
  }

  @Test
  public void T_getByte_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( (byte)0 , obj.getByte() );
  }

  @Test
  public void T_getShort_trueNum_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( (short)1 , obj.getShort() );
  }

  @Test
  public void T_getShort_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( (short)0 , obj.getShort() );
  }

  @Test
  public void T_getInt_trueNum_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( 1 , obj.getInt() );
  }

  @Test
  public void T_getInt_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( 0 , obj.getInt() );
  }

  @Test
  public void T_getLong_trueNum_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( 1L , obj.getLong() );
  }

  @Test
  public void T_getLong_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( 0L , obj.getLong() );
  }

  @Test
  public void T_getFloat_trueNum_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( 1f , obj.getFloat() );
  }

  @Test
  public void T_getFloat_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( 0f , obj.getFloat() );
  }

  @Test
  public void T_getDouble_trueNum_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertEquals( 1d , obj.getDouble() );
  }

  @Test
  public void T_getDouble_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( 0d , obj.getDouble() );
  }

  @Test
  public void T_getBoolean_true_withTrue() throws IOException {
    BooleanObj obj = new BooleanObj( true );
    assertTrue( obj.getBoolean() );
  }

  @Test
  public void T_getBoolean_falseNum_withFalse() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertFalse( obj.getBoolean() );
  }

  @Test
  public void T_getPrimitiveType_boolean() throws IOException {
    BooleanObj obj = new BooleanObj( false );
    assertEquals( PrimitiveType.BOOLEAN , obj.getPrimitiveType() );
  }

}
