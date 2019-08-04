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

public class TestJavaObjectToPrimitiveObject {

  @Test
  public void T_get_primitiveObject() {
    StringObj target = new StringObj( "a" );
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( target );
    assertTrue( ( obj instanceof StringObj ) );
  }

  @Test
  public void T_get_booleanObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( true );
    assertTrue( ( obj instanceof BooleanObj ) );
  }

  @Test
  public void T_get_byteObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( (byte)1 );
    assertTrue( ( obj instanceof ByteObj ) );
  }

  @Test
  public void T_get_bytesObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( "a".getBytes() );
    assertTrue( ( obj instanceof BytesObj ) );
  }

  @Test
  public void T_get_doubleObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( 1d );
    assertTrue( ( obj instanceof DoubleObj ) );
  }

  @Test
  public void T_get_floatObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( 1f );
    assertTrue( ( obj instanceof FloatObj ) );
  }

  @Test
  public void T_get_integerObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( 1 );
    assertTrue( ( obj instanceof IntegerObj ) );
  }

  @Test
  public void T_get_longObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( 1L );
    assertTrue( ( obj instanceof LongObj ) );
  }

  @Test
  public void T_get_shortObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( (short)1 );
    assertTrue( ( obj instanceof ShortObj ) );
  }

  @Test
  public void T_get_stringObject() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( "a" );
    assertTrue( ( obj instanceof StringObj ) );
  }

  @Test
  public void T_get_nullObject_withNull() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( null );
    assertTrue( ( obj instanceof NullObj ) );
  }

  @Test
  public void T_get_nullObject_withThis() {
    PrimitiveObject obj = JavaObjectToPrimitiveObject.get( this );
    assertTrue( ( obj instanceof NullObj ) );
  }

}
