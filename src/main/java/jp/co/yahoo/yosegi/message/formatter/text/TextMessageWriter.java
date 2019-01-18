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

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.formatter.IMessageWriter;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TextMessageWriter implements IMessageWriter {

  private final ByteArrayData buffer;
  private final ITextFormatter formatter;

  public TextMessageWriter( final IField schema ) throws IOException {
    buffer = new ByteArrayData();
    formatter = TextFormatterFactory.get( schema );
  }

  private byte[] createNewBytes() {
    byte[] newBytes = new byte[ buffer.getLength() ];

    byte[] bufferBytes = buffer.getBytes();
    System.arraycopy( bufferBytes , 0 , newBytes , 0 , newBytes.length );

    return newBytes;
  }

  @Override
  public byte[] create( final PrimitiveObject obj ) throws IOException {
    buffer.clear();
    formatter.write( buffer , obj );
    return createNewBytes();
  }

  @Override
  public byte[] create( final List<Object> array ) throws IOException {
    buffer.clear();
    formatter.write( buffer , array );
    return createNewBytes();
  }

  @Override
  public byte[] create( final Map<Object,Object> map ) throws IOException {
    buffer.clear();
    formatter.write( buffer , map );
    return createNewBytes();
  }

  @Override
  public byte[] create( final IParser parser ) throws IOException {
    buffer.clear();
    formatter.writeParser( buffer , null , parser );
    return createNewBytes();
  }

  @Override
  public void close() throws IOException {
    buffer.clear();
  }

}
