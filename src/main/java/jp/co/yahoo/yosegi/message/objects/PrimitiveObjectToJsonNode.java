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

import jp.co.yahoo.yosegi.util.EnumDispatcherFactory;

import java.io.IOException;
import java.util.Objects;

public class PrimitiveObjectToJsonNode {
  @FunctionalInterface
  private interface DispatchedFunc {
    public JsonNode apply(PrimitiveObject obj) throws IOException;
  }

  private static EnumDispatcherFactory.Func<PrimitiveType, DispatchedFunc> dispatcher;

  static {
    EnumDispatcherFactory<PrimitiveType, DispatchedFunc> sw =
        new EnumDispatcherFactory<>(PrimitiveType.class);
    sw.setDefault(obj -> NullNode.getInstance());
    sw.set(PrimitiveType.BOOLEAN, obj -> BooleanNode.valueOf(obj.getBoolean()));
    sw.set(PrimitiveType.BYTE,    obj -> IntNode.valueOf(obj.getInt()));
    sw.set(PrimitiveType.SHORT,   obj -> IntNode.valueOf(obj.getInt()));
    sw.set(PrimitiveType.INTEGER, obj -> IntNode.valueOf(obj.getInt()));
    sw.set(PrimitiveType.LONG,    obj -> new LongNode(   obj.getLong()));
    sw.set(PrimitiveType.FLOAT,   obj -> new DoubleNode( obj.getDouble()));
    sw.set(PrimitiveType.DOUBLE,  obj -> new DoubleNode( obj.getDouble()));
    sw.set(PrimitiveType.STRING,  obj -> new TextNode(   obj.getString()));
    sw.set(PrimitiveType.BYTES,   obj -> new BinaryNode( obj.getBytes()));
    dispatcher = sw.create();
  }


  /**
   * Convert PrimitiveObject to JsonNode.
   */
  public static JsonNode get(final PrimitiveObject obj) throws IOException {
    if (Objects.isNull(obj)) {
      return NullNode.getInstance();
    }
    return dispatcher.get(obj.getPrimitiveType()).apply(obj);
  }
}

