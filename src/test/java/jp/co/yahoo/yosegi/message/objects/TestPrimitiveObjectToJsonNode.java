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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestPrimitiveObjectToJsonNode {

  @Test
  public void T_get_null() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( null );
    assertTrue( ( node instanceof NullNode ) );
  }

  @Test
  public void T_get_boolean() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new BooleanObj( true ) );
    assertTrue( ( node instanceof BooleanNode ) );
  }

  @Test
  public void T_get_byte() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new ByteObj( (byte)1 ) );
    assertTrue( ( node instanceof IntNode ) );
  }

  @Test
  public void T_get_short() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new ShortObj( (short)1 ) );
    assertTrue( ( node instanceof IntNode ) );
  }

  @Test
  public void T_get_int() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new IntegerObj( 1 ) );
    assertTrue( ( node instanceof IntNode ) );
  }

  @Test
  public void T_get_long() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new LongObj( 1L ) );
    assertTrue( ( node instanceof LongNode ) );
  }

  @Test
  public void T_get_float() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new FloatObj( 1f ) );
    assertTrue( ( node instanceof DoubleNode ) );
  }

  @Test
  public void T_get_double() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new DoubleObj( 1d ) );
    assertTrue( ( node instanceof DoubleNode ) );
  }

  @Test
  public void T_get_string() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new StringObj( "a" ) );
    assertTrue( ( node instanceof TextNode ) );
  }

  @Test
  public void T_get_bytes() throws IOException {
    JsonNode node = PrimitiveObjectToJsonNode.get( new BytesObj( "a".getBytes() ) );
    assertTrue( ( node instanceof BinaryNode ) );
  }

}
