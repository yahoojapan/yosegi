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

package jp.co.yahoo.yosegi.encryptor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestEncryptionSettingNode {

  @Test
  public void T_createFromParser_equalsSetValue() throws IOException {
    String json = "{";
           json+=  "\"c_1\":{";
           json+=   "\"key_name\":\"key_1\"";
           json+=  "}";
           json+= "}";
    JacksonMessageReader reader = new JacksonMessageReader();
    Set<String> keyNameSet = new HashSet<String>();
    keyNameSet.add( "key_1" );
    IParser parser = reader.create( json );
    EncryptionSettingNode node = EncryptionSettingNode.createFromParser( parser , keyNameSet );
    EncryptionSettingNode c1 = node.getChildNode( "c_1" );
    assertTrue(  c1 != null );
    assertEquals( "key_1" , c1.getKeyName() );
  }

}
