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

import jp.co.yahoo.yosegi.message.parser.IMessageReader;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;

public class JavaMessageReader implements IMessageReader {

  public IParser create( final Object obj ) throws IOException {
    return JavaParserFactory.get( obj );
  }

  public IParser create( final byte[] message ) throws IOException {
    throw new UnsupportedOperationException( "Unsupport create( byte[] message )" );
  }

  public IParser create(
      final byte[] message , final int start , final int length ) throws IOException {
    throw new UnsupportedOperationException(
        "Unsupport create( byte[] message , int start , int length )" );
  }

}
