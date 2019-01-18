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

package jp.co.yahoo.yosegi.message.parser.text;

import jp.co.yahoo.yosegi.message.design.IContainerField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TextMapParser implements IParser {

  private final byte[] buffer;
  private final int start;
  private final int length;

  private final Map<String,PrimitiveObject> container;

  private final byte delimiter;
  private final byte fieldDelimiter;
  private final MapContainerField schema;

  private int readOffset;
  private int endOffset;

  /**
   * Creates an object to be parsed by assuming the range
   * of the specified byte array as the Map of key and value.
   */
  public TextMapParser(
      final byte[] buffer ,
      final int start ,
      final int length ,
      final MapContainerField schema ) throws IOException {
    this.buffer = buffer;
    this.start = start;
    this.length = length;

    readOffset = start;
    endOffset = start + length;

    container = new HashMap<String,PrimitiveObject>();

    Properties properties = schema.getProperties();

    if ( ! properties.containsKey( "delimiter" ) ) {
      throw new IOException(
          "Delimiter property is not found. Please set delimiter. Example 0x2c" );
    }

    if ( ! properties.containsKey( "field_delimiter" ) ) {
      throw new IOException(
          "Field delimiter property is not found. Please set field_delimiter. Example 0x2c" );
    }

    delimiter = (byte)( Integer.decode( properties.get( "delimiter" ) ).intValue() );
    fieldDelimiter = (byte)( Integer.decode( properties.get( "field_delimiter" ) ).intValue() );

    this.schema = schema;
  }

  private PrimitiveObject parse( final byte target ) throws IOException {
    if ( endOffset <= readOffset ) {
      return null;
    }

    for ( int i = readOffset ; i < endOffset ; i++ ) {
      if ( buffer[i] == target ) {
        PrimitiveObject retVal = new BytesObj( buffer , readOffset , ( i - readOffset ) );
        readOffset = i + 1;
        return retVal;
      }
    }
    PrimitiveObject retVal =
        new BytesObj( buffer , readOffset , ( length - ( readOffset - start ) ) );
    readOffset = endOffset;

    return retVal;
  }

  private boolean parseMap() throws IOException {
    PrimitiveObject keyObj = parse( fieldDelimiter );
    PrimitiveObject valueObj = parse( delimiter );

    if ( keyObj != null ) {
      if ( valueObj == null ) {
        valueObj = new BytesObj( new byte[0] );
      }
      String key = keyObj.getString();
      container.put(
          key ,
          TextPrimitiveConverter.textObjToPrimitiveObj( schema.get( key ) , valueObj ) );
    } else {
      return false;
    }

    return true;
  }

  private void parseAll() throws IOException {
    while ( parseMap() ) {};
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException {
    if ( containsKey( key ) ) {
      return container.get( key );
    }
    return null;
  }

  @Override
  public PrimitiveObject get( final int index ) throws IOException {
    return get( Integer.toString( index ) );
  }

  @Override
  public IParser getParser( final String key ) throws IOException {
    PrimitiveObject obj = get( key );
    if ( obj == null ) {
      return new TextNullParser();
    }
    byte[] parseTarget = obj.getBytes();
    return TextParserFactory.get( parseTarget , 0 , parseTarget.length , schema.get( key ) );
  }

  @Override
  public IParser getParser( final int index ) throws IOException {
    return getParser( Integer.toString( index ) );
  }

  @Override
  public String[] getAllKey() throws IOException {
    parseAll();
    String[] keys = new String[container.size()];
    int index = 0;
    for ( Map.Entry<String,PrimitiveObject> entry : container.entrySet() ) {
      keys[index] = entry.getKey();
      index++;
    }
    return keys;
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    while ( parseMap() && ! container.containsKey( key ) ) {};
    return container.containsKey( key );
  }

  @Override
  public int size() throws IOException {
    parseAll();
    return container.size();
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
    return hasParser( Integer.toString( index ) );
  }

  @Override
  public boolean hasParser( final String key ) throws IOException {
    IField childSchema = schema.get( key );
    return ( childSchema instanceof IContainerField );
  }

  @Override
  public Object toJavaObject() throws IOException {
    Map<String,Object> result = new HashMap<String,Object>();
    for ( String key : getAllKey() ) {
      if ( hasParser(key) ) {
        result.put( key , getParser(key).toJavaObject() );
      } else {
        result.put( key , get(key) );
      }
    }

    return result;
  }

}
