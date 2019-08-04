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
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.math.BigDecimal;

public class TestJsonNodeToPrimitiveObject {

  @Test
  public void T_get_stringObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( TextNode.valueOf( "a" ) );
    assertTrue( ( obj instanceof StringObj ) );
  }

  @Test
  public void T_get_booleanObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( BooleanNode.valueOf( true ) );
    assertTrue( ( obj instanceof BooleanObj ) );
  }

  @Test
  public void T_get_integerObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( IntNode.valueOf( 1 ) );
    assertTrue( ( obj instanceof IntegerObj ) );
  }

  @Test
  public void T_get_longObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( LongNode.valueOf( 1L ) );
    assertTrue( ( obj instanceof LongObj ) );
  }

  @Test
  public void T_get_doubleObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( DoubleNode.valueOf( 1d ) );
    assertTrue( ( obj instanceof DoubleObj ) );
  }

  @Test
  public void T_get_bigIntegerObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( BigIntegerNode.valueOf( new BigInteger( "100" ) ) );
    assertTrue( ( obj instanceof StringObj ) );
  }

  @Test
  public void T_get_decimalObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( DecimalNode.valueOf( new BigDecimal( "100" ) ) );
    assertTrue( ( obj instanceof StringObj ) );
  }

  @Test
  public void T_get_binaryObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( BinaryNode.valueOf( "a".getBytes() ) );
    assertTrue( ( obj instanceof BytesObj ) );
  }

  @Test
  public void T_get_binaryObject_withPOJONode() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( new POJONode( "a".getBytes() ) );
    assertTrue( ( obj instanceof BytesObj ) );
  }

  @Test
  public void T_get_nullObject() throws IOException {
    PrimitiveObject obj = JsonNodeToPrimitiveObject.get( NullNode.getInstance() );
    assertTrue( ( obj instanceof NullObj ) );
  }

}
