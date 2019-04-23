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

import jp.co.yahoo.yosegi.util.ObjectDispatchByClass;

import java.io.IOException;

public class JsonNodeToPrimitiveObject {
  @FunctionalInterface
  private interface DispatchedFunc {
    PrimitiveObject apply(JsonNode jsonNode) throws IOException;
  }

  private static ObjectDispatchByClass.Func<DispatchedFunc> dispatcher;

  static {
    ObjectDispatchByClass<DispatchedFunc> sw = new ObjectDispatchByClass<>();
    sw.setDefault(node -> new StringObj(node.toString()));
    sw.set(TextNode.class,    node -> new StringObj(((TextNode)node).textValue()));
    sw.set(BooleanNode.class, node -> new BooleanObj(((BooleanNode)node).booleanValue()));
    sw.set(IntNode.class,     node -> new IntegerObj(((IntNode)node).intValue()));
    sw.set(LongNode.class,    node -> new LongObj(((LongNode)node).longValue()));
    sw.set(DoubleNode.class,  node -> new DoubleObj(((DoubleNode)node).doubleValue()));
    sw.set(BigIntegerNode.class, node ->
        new StringObj(((BigIntegerNode)node).bigIntegerValue().toString()));
    sw.set(DecimalNode.class, node -> new StringObj(((DecimalNode)node).decimalValue().toString()));
    sw.set(BinaryNode.class,  node -> new BytesObj(((BinaryNode)node).binaryValue()));
    sw.set(POJONode.class,    node -> new BytesObj(((POJONode)node).binaryValue()));
    sw.set(NullNode.class,    node -> NullObj.getInstance());
    sw.set(MissingNode.class, node -> NullObj.getInstance());
    dispatcher = sw.create();
  }

  /**
   * Converts JsonNode to PrimitiveObject.
   */
  public static PrimitiveObject get(final JsonNode jsonNode) throws IOException {
    return dispatcher.get(jsonNode).apply(jsonNode);
  }
}

