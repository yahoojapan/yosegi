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

import java.nio.ByteBuffer;

public class TestAdditionalAuthenticationData {

  @Test
  public void T_createAAD_equalsSetValue_withIdentifier() {
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( identifier );

    byte[] aadBytes = aad.create( Module.BLOCK_META );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( aadBytes );
    byte[] newIdentifier = new byte[identifier.length];
    wrapBuffer.get( newIdentifier );
    assertEquals( new String( identifier ) , new String( newIdentifier ) );
    assertEquals( (short)0 , wrapBuffer.getShort() );
    assertEquals( AdditionalAuthenticationData.typeToByte( Module.BLOCK_META ) , wrapBuffer.get() );
    assertEquals( 0 , wrapBuffer.getInt() );

    aad.nextOrdinal();
    aadBytes = aad.create( Module.BLOCK_META );
    wrapBuffer = ByteBuffer.wrap( aadBytes );
    wrapBuffer.get( newIdentifier );
    assertEquals( new String( identifier ) , new String( newIdentifier ) );
    assertEquals( (short)0 , wrapBuffer.getShort() );
    assertEquals( AdditionalAuthenticationData.typeToByte( Module.BLOCK_META ) , wrapBuffer.get() );
    assertEquals( 1 , wrapBuffer.getInt() );

    aad.nextBlock();
    aadBytes = aad.create( Module.BLOCK_META );
    wrapBuffer = ByteBuffer.wrap( aadBytes );
    wrapBuffer.get( newIdentifier );
    assertEquals( new String( identifier ) , new String( newIdentifier ) );
    assertEquals( (short)1 , wrapBuffer.getShort() );
    assertEquals( AdditionalAuthenticationData.typeToByte( Module.BLOCK_META ) , wrapBuffer.get() );
    assertEquals( 0 , wrapBuffer.getInt() );
  }

  @Test
  public void T_createAAD_equalsSetValue_withPrefixAndIdentifier() {
    byte[] prefix = "prefix".getBytes();
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( prefix , identifier );

    byte[] aadBytes = aad.create( Module.BLOCK_META );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( aadBytes );
    byte[] newPrefix = new byte[prefix.length];
    byte[] newIdentifier = new byte[identifier.length];
    wrapBuffer.get( newPrefix );
    wrapBuffer.get( newIdentifier );
    assertEquals( new String( prefix ) , new String( newPrefix ) );
    assertEquals( new String( identifier ) , new String( newIdentifier ) );
    assertEquals( (short)0 , wrapBuffer.getShort() );
    assertEquals( AdditionalAuthenticationData.typeToByte( Module.BLOCK_META ) , wrapBuffer.get() );
    assertEquals( 0 , wrapBuffer.getInt() );
  }

}
