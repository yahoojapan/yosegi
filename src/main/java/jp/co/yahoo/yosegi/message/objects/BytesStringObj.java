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

package jp.co.yahoo.yosegi.message.objects;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class BytesStringObj extends BytesObj {

  private static final String DEFAULT_CHAR_SET = "UTF-8";

  private final String charsetName;

  public BytesStringObj() {
    super();
    charsetName = DEFAULT_CHAR_SET;
  }

  public BytesStringObj( final byte[] data ) {
    super( data );
    charsetName = DEFAULT_CHAR_SET;
  }

  public BytesStringObj( final byte[] data , final int start , final int length ) {
    super( data , start , length );
    charsetName = DEFAULT_CHAR_SET;
  }

  public BytesStringObj( final byte[] data , final String charsetName ) {
    super( data );
    this.charsetName = charsetName;
  }

  public BytesStringObj(
      final byte[] data ,
      final int start ,
      final int length ,
      final String charsetName ) {
    super( data , start , length );
    this.charsetName = charsetName;
  }

  @Override
  public String getString() throws IOException {
    return new String( getBytes() , charsetName );
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.STRING;
  }

}
