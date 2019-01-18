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

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.IStreamReader;
import jp.co.yahoo.yosegi.util.ByteLineReader;

import java.io.IOException;
import java.io.InputStream;

public class TextStreamReader implements IStreamReader {

  private final ByteLineReader lineReader;
  private final TextMessageReader messageReader;

  public TextStreamReader(
      final InputStream in , final IField schema ) throws IOException {
    lineReader = new ByteLineReader( in );
    messageReader = new TextMessageReader( schema );
  }

  @Override
  public boolean hasNext() throws IOException {
    return lineReader.hasNext();
  }

  @Override
  public IParser next() throws IOException {
    int currentLength = lineReader.readLine();
    byte[] currentBytes = lineReader.get();
    return messageReader.create( currentBytes , 0 , currentLength);
  }

  @Override
  public void close() throws IOException {
    lineReader.close();
  }

}
