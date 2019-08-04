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

package jp.co.yahoo.yosegi.message.formatter.text;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestTextStringFormatter {

  @Test
  public void T_write_stringText_withByteArray() throws IOException {
    TextStringFormatter formatter = new TextStringFormatter();
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , new String( "abc" ).getBytes() );
    assertEquals( "abc" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_write_stringText_withString() throws IOException {
    TextStringFormatter formatter = new TextStringFormatter();
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , new String( "abc" ) );
    assertEquals( "abc" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_write_stringText_withBytesObj() throws IOException {
    TextStringFormatter formatter = new TextStringFormatter();
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , new BytesObj( new String( "abc" ).getBytes() ) );
    assertEquals( "abc" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_write_stringText_withNull() throws IOException {
    TextStringFormatter formatter = new TextStringFormatter();
    ByteArrayData buffer = new ByteArrayData();
    formatter.write( buffer , null );
    assertEquals( "" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_writeParser_stringText_withBytesObj() throws IOException {
    TextStringFormatter formatter = new TextStringFormatter();
    ByteArrayData buffer = new ByteArrayData();
    formatter.writeParser( buffer ,  new BytesObj( new String( "abc" ).getBytes() ) , null );
    assertEquals( "abc" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

  @Test
  public void T_writeParser_stringText_withNull() throws IOException {
    TextStringFormatter formatter = new TextStringFormatter();
    ByteArrayData buffer = new ByteArrayData();
    formatter.writeParser( buffer , null , null );
    assertEquals( "" , new String( buffer.getBytes() , 0 , buffer.getLength() ) );
  }

}
