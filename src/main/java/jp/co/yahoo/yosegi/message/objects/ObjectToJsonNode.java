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

import jp.co.yahoo.yosegi.util.ObjectDispatchByClass;

import java.io.IOException;
import java.util.Objects;

public class ObjectToJsonNode {
  @FunctionalInterface
  private interface DispatchedFunc {
    JsonNode apply(Object obj) throws IOException;
  }

  private static ObjectDispatchByClass.Func<DispatchedFunc> dispatcher;

  static {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.setDefault(obj -> new TextNode(obj.toString()));
    sw.set(PrimitiveObject.class, obj -> PrimitiveObjectToJsonNode.get((PrimitiveObject)obj));
    sw.set(String.class, obj ->  new TextNode((String)obj));
    sw.set(Boolean.class, obj -> BooleanNode.valueOf((Boolean)obj));
    sw.set(Short.class, obj ->   IntNode.valueOf(( (Short)obj).intValue()));
    sw.set(Integer.class, obj -> IntNode.valueOf((Integer)obj));
    sw.set(Long.class, obj ->    new LongNode((Long)obj));
    sw.set(Float.class, obj ->   new DoubleNode(( (Float)obj).doubleValue()));
    sw.set(Double.class, obj ->  new DoubleNode((Double)obj));
    sw.set(byte[].class, obj ->  new BinaryNode((byte[])obj));
    dispatcher = sw.create();
  }

  /**
   * Judge Java objects and create JsonNode.
   */
  public static JsonNode get(final Object obj) throws IOException {
    return Objects.isNull(obj) ? NullNode.getInstance() : dispatcher.get(obj).apply(obj);
  }
}

