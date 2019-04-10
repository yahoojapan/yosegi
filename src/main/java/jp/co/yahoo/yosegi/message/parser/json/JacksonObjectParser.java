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

package jp.co.yahoo.yosegi.message.parser.json;

import com.fasterxml.jackson.databind.node.ObjectNode;

import jp.co.yahoo.yosegi.message.objects.JsonNodeToPrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JacksonObjectParser implements IParser {

  private final ObjectNode objectNode;

  public JacksonObjectParser( final ObjectNode objectNode ) {
    this.objectNode = objectNode;
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException {
    return JsonNodeToPrimitiveObject.get( objectNode.path( key ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return get( Integer.toString( index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    return JsonNodeToParser.get( objectNode.path( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    return getParser( Integer.toString( index ) );
  }

  @Override
  public String[] getAllKey() throws IOException {
    Iterator<String> iterator = objectNode.fieldNames();
    List<String> list = new ArrayList<String>();
    while ( iterator.hasNext() ) {
      list.add( (String)iterator.next() );
    }
    return list.toArray( new String[ list.size() ] );
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return Objects.nonNull(objectNode.get(key));
  }

  @Override
  public int size() throws IOException {
    return objectNode.size();
  }

  @Override
  public boolean isArray() throws IOException {
    return false;
  }

  @Override
  public boolean isMap() throws IOException {
    return false;
  }

  @Override
  public boolean isStruct() throws IOException {
    return true;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    return hasParser( Integer.toString( index ) );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    return JsonNodeToParser.hasParser( objectNode.path( key ) );
  }

  @Override
  public Object toJavaObject() throws IOException {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String key : getAllKey()) {
      Object obj = hasParser(key) ? getParser(key).toJavaObject() : get(key);
      result.put(key, obj);
    }
    return result;
  }
}

