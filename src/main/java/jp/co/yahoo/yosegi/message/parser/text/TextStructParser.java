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
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.NullObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TextStructParser implements IParser {

  private final byte[] buffer;
  private final int start;
  private final int length;

  private final Map<String,PrimitiveObject> container;
  private final String[] keys;
  private final int fieldNumber;

  private final byte delimiter;
  private final StructContainerField schema;

  private int readOffset;
  private int endOffset;

  /**
   * Creates an object to be parsed by assuming the range
   * of the specified byte array as the Struct.
   */
  public TextStructParser( final byte[] buffer , final int start ,
      final int length , final StructContainerField schema ) throws IOException {
    this.buffer = buffer;
    this.start = start;
    this.length = length;

    readOffset = start;
    endOffset = start + length;

    container = new HashMap<String,PrimitiveObject>();
    keys = schema.getKeys();
    fieldNumber = keys.length;

    Properties properties = schema.getProperties();

    if ( ! properties.containsKey( "delimiter" ) ) {
      throw new IOException(
          "Delimiter property is not found. Please set struct delimiter. Example 0x2c" );
    }

    delimiter = (byte)( Integer.decode( properties.get( "delimiter" ) ).intValue() );

    this.schema = schema;
  }

  private boolean parse() throws IOException {
    if ( endOffset <= readOffset ) {
      return false;
    }

    for ( int i = readOffset ; i < endOffset && container.size() < fieldNumber ; i++ ) {
      if ( buffer[i] == delimiter ) {
        container.put(
            keys[container.size()] ,
            TextPrimitiveConverter.textObjToPrimitiveObj(
              schema.get( keys[container.size()] ) ,
              new BytesObj( buffer , readOffset , ( i - readOffset ) )
            ) );
        readOffset = i + 1;
        return true;
      }
    }
    container.put( 
        keys[container.size()] , 
        TextPrimitiveConverter.textObjToPrimitiveObj(
          schema.get(
            keys[container.size()] 
          ) ,
          new BytesObj(
            buffer ,
            readOffset ,
            ( length - ( readOffset - start ) )
          ) 
        ) );
    readOffset = endOffset;

    return true;
  }

  private void parseAll() throws IOException {
    while ( parse() ) {};
  }

  @Override
  public PrimitiveObject get( final String key ) throws IOException {
    if ( ! containsKey( key ) ) {
      return null;
    }

    PrimitiveObject obj = container.get( key );
    if ( obj == null ) {
      return NullObj.getInstance();
    }

    return obj;
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
    if ( index < fieldNumber ) {
      return new TextNullParser();
    }
    return getParser( keys[index] );
  }

  @Override
  public String[] getAllKey() throws IOException {
    return schema.getKeys();
  }

  @Override
  public boolean containsKey( final String key ) throws IOException {
    while ( parse() && ! container.containsKey( key ) ) {};
    return schema.containsKey( key );
  }

  @Override
  public int size() throws IOException {
    return fieldNumber;
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
    IField childSchema = schema.get( key );
    return ( childSchema instanceof IContainerField);
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
