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

import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TextStructFormatter implements ITextFormatter {

  private final String[] keys;
  private final List<ITextFormatter> childFormatterContainer;
  private final byte[] delimiter;

  /**
   * Create an object to write as a Struct from the schema.
   */
  public TextStructFormatter( final StructContainerField schema ) throws IOException {
    childFormatterContainer = new ArrayList<ITextFormatter>();
    keys = schema.getKeys();
    for ( String key : keys ) {
      childFormatterContainer.add( TextFormatterFactory.get( schema.get( key ) ) );
    }

    Properties properties = schema.getProperties();
    if ( ! properties.containsKey( "delimiter" ) ) {
      throw new IOException(
        "Delimiter property is not found. Please set delimiter. Example 0x2c" );
    }

    delimiter = new byte[1];
    delimiter[0] = (byte)( Integer.decode( properties.get( "delimiter" ) ).intValue() );
  }

  /**
   * Create Struct byte array from Java object.
   */
  public void write(final ByteArrayData buffer , final Object obj ) throws IOException {
    if ( ! ( obj instanceof Map ) ) {
      return;
    }
    Map<Object,Object> mapObj = (Map<Object,Object>)obj;
    for ( int i = 0 ; i < keys.length ; i++ ) {
      if ( i != 0 ) {
        buffer.append( delimiter , 0 , delimiter.length );
      }
      ITextFormatter childFormatter = childFormatterContainer.get(i);
      childFormatter.write( buffer , mapObj.get( keys[i] ) );
    }
  }

  /**
   * Create Struct byte array from IParser.
   */
  public void writeParser(
      final ByteArrayData buffer ,
      final PrimitiveObject obj ,
      final IParser parser ) throws IOException {
    for ( int i = 0 ; i < keys.length ; i++ ) {
      if ( i != 0 ) {
        buffer.append( delimiter , 0 , delimiter.length );
      }
      ITextFormatter childFormatter = childFormatterContainer.get(i);
      childFormatter.writeParser( buffer , parser.get( keys[i] ) , parser.getParser( keys[i] ) );
    }
  }

}
