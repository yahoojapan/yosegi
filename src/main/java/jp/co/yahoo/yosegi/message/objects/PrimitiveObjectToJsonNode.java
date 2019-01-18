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

import java.io.IOException;

public class PrimitiveObjectToJsonNode {

  /**
   * Convert PrimitiveObject to JsonNode.
   */
  public static JsonNode get( final PrimitiveObject obj ) throws IOException {
    if ( obj == null ) {
      return NullNode.getInstance();
    }
    switch ( obj.getPrimitiveType() ) {
      case BOOLEAN:
        return BooleanNode.valueOf( obj.getBoolean() );
      case BYTE:
        return IntNode.valueOf( obj.getInt() );
      case SHORT:
        return IntNode.valueOf( obj.getInt() );
      case INTEGER:
        return IntNode.valueOf( obj.getInt() );
      case LONG:
        return new LongNode( obj.getLong() );
      case FLOAT:
        return new DoubleNode( obj.getDouble() );
      case DOUBLE:
        return new DoubleNode( obj.getDouble() );
      case STRING:
        return new TextNode( obj.getString() );
      case BYTES:
        return new BinaryNode( obj.getBytes() );
      default:
        return NullNode.getInstance();
    }
  }

}
