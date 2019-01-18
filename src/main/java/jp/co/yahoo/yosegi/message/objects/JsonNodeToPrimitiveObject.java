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

import java.io.IOException;

public class JsonNodeToPrimitiveObject {

  /**
   * Converts JsonNode to PrimitiveObject.
   */
  public static PrimitiveObject get( final JsonNode jsonNode ) throws IOException {
    if ( jsonNode instanceof TextNode ) {
      return new StringObj( ( (TextNode)jsonNode ).textValue() );
    } else if ( jsonNode instanceof BooleanNode ) {
      return new BooleanObj( ( (BooleanNode)jsonNode ).booleanValue() );
    } else if ( jsonNode instanceof IntNode ) {
      return new IntegerObj( ( (IntNode)jsonNode ).intValue() );
    } else if ( jsonNode instanceof LongNode ) {
      return new LongObj( ( (LongNode)jsonNode ).longValue() );
    } else if ( jsonNode instanceof DoubleNode ) {
      return new DoubleObj( ( (DoubleNode)jsonNode ).doubleValue() );
    } else if ( jsonNode instanceof BigIntegerNode ) {
      return new StringObj( ( (BigIntegerNode)jsonNode ).bigIntegerValue().toString() );
    } else if ( jsonNode instanceof DecimalNode ) {
      return new StringObj( ( (DecimalNode)jsonNode ).decimalValue().toString() );
    } else if ( jsonNode instanceof BinaryNode ) {
      return new BytesObj( ( (BinaryNode)jsonNode ).binaryValue() );
    } else if ( jsonNode instanceof POJONode ) {
      return new BytesObj( ( (POJONode)jsonNode ).binaryValue() );
    } else if ( jsonNode instanceof NullNode ) {
      return NullObj.getInstance();
    } else if ( jsonNode instanceof MissingNode ) {
      return NullObj.getInstance();
    } else {
      return new StringObj( jsonNode.toString() );
    }
  }

}
