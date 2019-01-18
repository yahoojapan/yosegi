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
import jp.co.yahoo.yosegi.message.formatter.IStreamWriter;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public class TextStreamWriter implements IStreamWriter {

  private static final byte[] LINE_DELIMITER = new byte[]{ (byte)'\n' };

  private final OutputStream out;
  private final IMessageWriter messageWriter;

  public TextStreamWriter( final OutputStream out , final IField schema ) throws IOException {
    this.out = out;
    messageWriter = new TextMessageWriter( schema );
  }

  @Override
  public void write( final PrimitiveObject obj ) throws IOException {
    byte[] message = messageWriter.create( obj );
    out.write( message , 0 , message.length );
    out.write( LINE_DELIMITER , 0 , LINE_DELIMITER.length );
  }

  @Override
  public void write( final List<Object> array ) throws IOException {
    byte[] message = messageWriter.create( array );
    out.write( message , 0 , message.length );
    out.write( LINE_DELIMITER , 0 , LINE_DELIMITER.length );
  }

  @Override
  public void write( final Map<Object,Object> map ) throws IOException {
    byte[] message = messageWriter.create( map );
    out.write( message , 0 , message.length );
    out.write( LINE_DELIMITER , 0 , LINE_DELIMITER.length );
  }

  @Override
  public void write( final IParser parser ) throws IOException {
    byte[] message = messageWriter.create( parser );
    out.write( message , 0 , message.length );
    out.write( LINE_DELIMITER , 0 , LINE_DELIMITER.length );
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

}
