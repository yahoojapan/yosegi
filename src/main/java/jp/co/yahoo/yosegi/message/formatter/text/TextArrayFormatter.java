/*
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

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.util.List;

public class TextArrayFormatter implements ITextFormatter {

  private final ITextFormatter childFormatter;
  private final byte[] delimiter;

  /**
   * Create an object to write as a Array from the schema.
   */
  public TextArrayFormatter(final ArrayContainerField schema) throws IOException {
    childFormatter = TextFormatterFactory.get(schema.getField());

    Properties properties = schema.getProperties();
    if ( ! properties.containsKey("delimiter") ) {
      throw new IOException(
          "Delimiter property is not found. Please set array delimiter. Example 0x2c" );
    }

    delimiter = new byte[1];
    delimiter[0] = (byte)( Integer.decode(properties.get("delimiter")).intValue() );
  }

  /**
   * Create Array byte array from Java object.
   */
  public void write( final ByteArrayData buffer, final Object obj ) throws IOException {
    if ( ! ( obj instanceof List ) ) {
      return;
    }

    List<Object> objList = (List<Object>)obj;
    int num = objList.size();
    if ( num <= 0 ) {
      return;
    }

    childFormatter.write( buffer, objList.get(0) );
    for ( int i = 1 ; i < num ; i++ ) {
      buffer.append( delimiter, 0, delimiter.length );
      childFormatter.write( buffer, objList.get(i) );
    }
  }

  /**
   * Create Array byte array from IParser.
   */
  public void writeParser(
      final ByteArrayData buffer ,
      final PrimitiveObject obj ,
      final IParser parser ) throws IOException {
    int num = parser.size();
    if ( num <= 0 ) {
      return;
    }

    for ( int i = 1 ; i < num ; i++ ) {
      buffer.append(delimiter, 0, delimiter.length);
      childFormatter.writeParser( buffer, parser.get(i), parser.getParser(i) );
    }
  }
}
