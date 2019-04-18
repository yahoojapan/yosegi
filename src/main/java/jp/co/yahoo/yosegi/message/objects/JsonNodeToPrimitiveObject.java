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

import jp.co.yahoo.yosegi.util.SwitchDispatcherFactory;

import java.io.IOException;

public class JsonNodeToPrimitiveObject {
  @FunctionalInterface
  private interface DispatchedFunc {
    public PrimitiveObject apply(JsonNode jsonNode) throws IOException;
  }

  private static SwitchDispatcherFactory.Func<Class, DispatchedFunc> dispatcher;

  static {
    /* CAUTION:
     * this structure is not the same function from original.
     * If there is a class derived from the following class,
     * it is necessary to branch by if then else if statement.
     */
    SwitchDispatcherFactory<Class, DispatchedFunc> sw = new SwitchDispatcherFactory();
    sw.setDefault(jsonNode -> new StringObj(jsonNode.toString()));
    sw.set(TextNode.class,    jsonNode -> new StringObj(((TextNode)jsonNode).textValue()));
    sw.set(BooleanNode.class, jsonNode -> new BooleanObj(((BooleanNode)jsonNode).booleanValue()));
    sw.set(IntNode.class,     jsonNode -> new IntegerObj(((IntNode)jsonNode).intValue()));
    sw.set(LongNode.class,    jsonNode -> new LongObj(((LongNode)jsonNode).longValue()));
    sw.set(DoubleNode.class,  jsonNode -> new DoubleObj(((DoubleNode)jsonNode).doubleValue()));
    sw.set(BinaryNode.class,  jsonNode -> new BytesObj(((BinaryNode)jsonNode).binaryValue()));
    sw.set(POJONode.class,    jsonNode -> new BytesObj(((POJONode)jsonNode).binaryValue()));
    sw.set(NullNode.class,    jsonNode -> NullObj.getInstance());
    sw.set(MissingNode.class, jsonNode -> NullObj.getInstance());
    sw.set(BigIntegerNode.class,
        jsonNode -> new StringObj(((BigIntegerNode)jsonNode).bigIntegerValue().toString()));
    sw.set(DecimalNode.class,
        jsonNode -> new StringObj(((DecimalNode)jsonNode).decimalValue().toString()));
    dispatcher = sw.create();
  }


  /**
   * Converts JsonNode to PrimitiveObject.
   */
  public static PrimitiveObject get(final JsonNode jsonNode) throws IOException {
    return dispatcher.get(jsonNode.getClass()).apply(jsonNode);
  }
}

