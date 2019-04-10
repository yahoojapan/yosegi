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

package jp.co.yahoo.yosegi.message.parser.java;

import jp.co.yahoo.yosegi.message.objects.JavaObjectToPrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class JavaMapParser implements IParser {

  private final Map<Object,Object> map;

  public JavaMapParser( final Map<Object,Object> map ) {
    this.map = map;
  }

  @Override
  public PrimitiveObject get(final String key ) throws IOException {
    return JavaObjectToPrimitiveObject.get( map.get( key ) );
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return NullObj.getInstance();
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    return JavaParserFactory.get( map.get( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    return new JavaNullParser();
  }

  @Override
  public String[] getAllKey() throws IOException {
    String[] keys = new String[map.size()];

    Iterator<Object> keyIterator = map.keySet().iterator();
    int index = 0;
    while ( keyIterator.hasNext() ) {
      keys[index] = keyIterator.next().toString();
      index++;
    }

    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    return map.containsKey( key );
  }

  @Override
  public int size() throws IOException {
    return map.size();
  }

  @Override
  public boolean isArray() throws IOException {
    return false;
  }

  @Override
  public boolean isMap() throws IOException {
    return true;
  }

  @Override
  public boolean isStruct() throws IOException {
    return false;
  }

  @Override
  public boolean hasParser( final int index ) throws IOException {
    return false;
  }

  @Override
  public boolean hasParser(final String key) throws IOException {
    return (IParser.class.isInstance(JavaParserFactory.get(map.get(key))));
  }

  @Override
  public Object toJavaObject() throws IOException {
    Map<Object,Object> result = new LinkedHashMap<Object,Object>();
    result.putAll( map );
    return result;
  }

}
