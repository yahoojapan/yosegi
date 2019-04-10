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
import jp.co.yahoo.yosegi.message.parser.IMessageReader;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;

public class TextMessageReader implements IMessageReader {

  private final IField schema;

  public TextMessageReader( final IField schema ) throws IOException {
    this.schema = schema;
  }

  @Override
  public IParser create( final byte[] message ) throws IOException {
    return create( message , 0 , message.length );
  }

  @Override
  public IParser create(
      final byte[] message , final int start , final int length ) throws IOException {
    return TextParserFactory.get( message , start , length , schema );
  }
}

