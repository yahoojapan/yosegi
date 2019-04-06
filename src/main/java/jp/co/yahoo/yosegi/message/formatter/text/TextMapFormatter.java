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

package jp.co.yahoo.yosegi.message.formatter.text;

import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TextMapFormatter implements ITextFormatter {

  private final ITextFormatter defaultFormatter;
  private final Map<String,ITextFormatter> childFormatterContainer;
  private final byte[] delimiter;
  private final byte[] fieldDelimiter;

  /**
   * Create an object to write as a Map from the schema.
   */
  public TextMapFormatter( final MapContainerField schema ) throws IOException {
    defaultFormatter = TextFormatterFactory.get( schema.getField() );
    childFormatterContainer = new HashMap<String,ITextFormatter>();
    for ( String key : schema.getKeys() ) {
      childFormatterContainer.put( key , TextFormatterFactory.get( schema.get( key ) ) );
    }

    Properties properties = schema.getProperties();

    if ( ! properties.containsKey( "delimiter" ) ) {
      throw new IOException(
          "Delimiter property is not found. Please set delimiter. Example 0x2c" );
    }

    if ( ! properties.containsKey( "field_delimiter" ) ) {
      throw new IOException(
          "Field delimiter property is not found. Please set field_delimiter. Example 0x2c" );
    }

    delimiter = new byte[1];
    delimiter[0] = (byte)( Integer.decode( properties.get( "delimiter" ) ).intValue() );

    fieldDelimiter = new byte[1];
    fieldDelimiter[0] = (byte)( Integer.decode( properties.get( "field_delimiter" ) ).intValue() );
  }

  /**
   * Create Map byte array from Java object.
   */
  public void write( final ByteArrayData buffer , final Object obj ) throws IOException {
    if ( ! ( obj instanceof Map ) ) {
      return;
    }
    Map<Object,Object> mapObj = (Map<Object,Object>)obj;
    int index = 0;
    for ( Map.Entry<Object,Object> entry : mapObj.entrySet() ) {
      String key = entry.getKey().toString();
      ITextFormatter childFormatter = childFormatterContainer.get( key );
      if ( Objects.isNull(childFormatter) ) {
        childFormatter = defaultFormatter;
      }
      if ( index != 0 ) {
        buffer.append( delimiter );
      }
      buffer.append( key.getBytes( "UTF-8" ) );
      buffer.append( fieldDelimiter );
      childFormatter.write( buffer , entry.getValue() );

      index++;
    }
  }

  /**
   * Create Map byte array from IParser.
   */
  public void writeParser(
      final ByteArrayData buffer ,
      final PrimitiveObject obj ,
      final IParser parser ) throws IOException {
    int index = 0;
    for ( String key : parser.getAllKey() ) {
      ITextFormatter childFormatter = childFormatterContainer.get( key );
      if ( Objects.isNull(childFormatter) ) {
        childFormatter = defaultFormatter;
      }
      if ( index != 0 ) {
        buffer.append( delimiter );
      }
      buffer.append( key.getBytes( "UTF-8" ) );
      buffer.append( fieldDelimiter );
      childFormatter.writeParser( buffer , parser.get( key ) , parser.getParser( key ) );

      index++;
    }
  }

}
