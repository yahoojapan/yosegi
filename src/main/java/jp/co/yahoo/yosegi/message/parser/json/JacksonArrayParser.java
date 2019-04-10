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

import com.fasterxml.jackson.databind.node.ArrayNode;

import jp.co.yahoo.yosegi.message.objects.JsonNodeToPrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class JacksonArrayParser implements IParser {

  private final ArrayNode arrayNode;

  public JacksonArrayParser( final ArrayNode arrayNode ) {
    this.arrayNode = arrayNode;
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException {
    return get( Integer.parseInt( key ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return JsonNodeToPrimitiveObject.get( arrayNode.path( index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    return getParser( Integer.parseInt( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    return JsonNodeToParser.get( arrayNode.path( index ) );
  }

  @Override
  public String[] getAllKey() throws IOException {
    String[] keys = new String[size()];
    IntStream.range(0, size()).forEach(i -> keys[i] = Integer.toString(i));
    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return false;
  }

  @Override
  public int size() throws IOException {
    return arrayNode.size();
  }

  @Override
  public boolean isArray() throws IOException {
    return true;
  }

  @Override
  public boolean isMap() throws IOException {
    return false;
  }

  @Override
  public boolean isStruct() throws IOException {
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    return JsonNodeToParser.hasParser( arrayNode.path( index ) );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    return hasParser( Integer.parseInt( key ) );
  }

  @Override
  public Object toJavaObject() throws IOException {
    List<Object> result = new ArrayList<Object>();
    if ( hasParser(0) ) {
      IntStream.range( 0 , size() )
          .forEach( i -> {
            try {
              result.add( getParser(i).toJavaObject() );
            } catch ( IOException ex ) {
              throw new UncheckedIOException( "IOException in lambda." , ex );
            }
          } );
    } else {
      IntStream.range( 0 , size() )
          .forEach( i -> {
            try {
              result.add( get(i) );
            } catch ( IOException ex ) {
              throw new UncheckedIOException( "IOException in lambda." , ex );
            }
          } );
    }

    return result;
  }

}
