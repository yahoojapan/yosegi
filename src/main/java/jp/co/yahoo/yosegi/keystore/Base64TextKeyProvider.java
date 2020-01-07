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

package jp.co.yahoo.yosegi.keystore;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.encryptor.EncryptionKey;

import java.io.IOException;
import java.util.Base64;

public class Base64TextKeyProvider implements IKeyProvider {

  private EncryptionKey key;

  @Override
  public void setup( final Configuration config ) throws IOException {
    if ( ! config.containsKey( "text" ) ) {
      throw new IOException( "Missing required parameter \"text\"." );
    }
    key = new EncryptionKey( Base64.getDecoder().decode( config.get( "text" ) ) );
  }

  @Override
  public EncryptionKey getKey() throws IOException {
    return key;
  }

}
