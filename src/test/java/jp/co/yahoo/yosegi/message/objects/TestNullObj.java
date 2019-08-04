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

public class TestNullObj {

  @Test
  public void T_get_null() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    Object result = obj.get();
    assertEquals( null , result );
  }

  @Test
  public void T_getString_null() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    String result = obj.getString();
    assertEquals( null , result );
  }

  @Test
  public void T_getBytes_null() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    byte[] result = obj.getBytes();
    assertEquals( null , result );
  }

  @Test
  public void T_getByte_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getByte();
      }
    );
  }

  @Test
  public void T_getShort_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getShort();
      }
    );
  }

  @Test
  public void T_getInt_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getInt();
      }
    );
  }

  @Test
  public void T_getLong_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getLong();
      }
    );
  }

  @Test
  public void T_getFloat_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getFloat();
      }
    );
  }

  @Test
  public void T_getDouble_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NumberFormatException.class ,
      () -> {
        obj.getDouble();
      }
    );
  }

  @Test
  public void T_getBoolean_throwsException() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertThrows( NullPointerException.class ,
      () -> {
        obj.getBoolean();
      }
    );
  }

  @Test
  public void T_getPrimitiveType_equalsType() throws IOException {
    PrimitiveObject obj = NullObj.getInstance();
    assertEquals( PrimitiveType.NULL , obj.getPrimitiveType() );
  }

}
