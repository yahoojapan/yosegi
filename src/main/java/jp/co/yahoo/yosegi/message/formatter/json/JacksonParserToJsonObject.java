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

package jp.co.yahoo.yosegi.message.formatter.json;

import com.fasterxml.jackson.databind.JsonNode;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonNullParser;

import java.io.IOException;

public class JacksonParserToJsonObject {

  public static final IJacksonFormatter arrayFormatter = new JacksonArrayFormatter();
  public static final IJacksonFormatter objectFormatter = new JacksonObjectFormatter();

  /**
   * Create a JsonNode from an ArrayParser.
   */
  public static JsonNode getFromArrayParser( final IParser parser ) throws IOException {
    if ( parser == null ) {
      return arrayFormatter.writeParser( new JacksonNullParser() );
    }
    return arrayFormatter.writeParser( parser );
  }

  /**
   * Create a JsonNode from an ObjectParser.
   */
  public static JsonNode getFromObjectParser( final IParser parser ) throws IOException {
    if ( parser == null ) {
      return objectFormatter.writeParser( new JacksonNullParser() );
    }
    return objectFormatter.writeParser( parser );
  }

}
