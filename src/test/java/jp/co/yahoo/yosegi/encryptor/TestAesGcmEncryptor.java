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

import java.io.IOException;
import java.util.stream.Stream;

public class TestAesGcmEncryptor {

  public static Stream<Arguments> keys() {
    return Stream.of(
      arguments( "sercretkeyaes128".getBytes() ),
      arguments( "test sercret key aes192.".getBytes() )
    );
  }

  @ParameterizedTest
  @MethodSource( "keys" )
  public void T_encryptAndDecrypt_equals( byte[] skey ) throws IOException {
    EncryptionKey key = new EncryptionKey( skey );
    byte[] aad = "this is aad".getBytes();
    AesGcmEncryptor encryptor = new AesGcmEncryptor( key , aad );

    byte[] text = "Hello world.".getBytes();
    byte[] cipherText = encryptor.encrypt( text , 0 , text.length );
    
    assertEquals( cipherText.length ,
        text.length + AesGcmEncryptor.GCM_NONCE_LENGTH + AesGcmEncryptor.GCM_TAG_LENGTH );
    assertNotEquals( new String( text ) , new String( cipherText ) );

    byte[] newText = encryptor.decrypt( cipherText , 0 , cipherText.length );
    assertEquals( text.length , newText.length );
    assertEquals( new String( text ) , new String( newText ) );
  }

  @Test
  public void T_encryptAndDecrypt_throwIOException_ModifyTag() throws IOException {
    EncryptionKey key = new EncryptionKey( "this is test sercret key".getBytes() );
    byte[] aad = "this is aad".getBytes();
    AesGcmEncryptor encryptor = new AesGcmEncryptor( key , aad );

    byte[] text = "Hello world.".getBytes();
    byte[] cipherText = encryptor.encrypt( text , 0 , text.length );

    assertEquals( cipherText.length ,
        text.length + AesGcmEncryptor.GCM_NONCE_LENGTH + AesGcmEncryptor.GCM_TAG_LENGTH );
    assertNotEquals( new String( text ) , new String( cipherText ) );
    cipherText[ cipherText.length - 1 ] += 1;

    assertThrows( IOException.class ,
      () -> {
        encryptor.decrypt( cipherText , 0 , cipherText.length );
      }
    );
  }

  @Test
  public void T_encryptAndDecrypt_throwIOException_ModifyNonce() throws IOException {
    EncryptionKey key = new EncryptionKey( "this is test sercret key".getBytes() );
    byte[] aad = "this is aad".getBytes();
    AesGcmEncryptor encryptor = new AesGcmEncryptor( key , aad );

    byte[] text = "Hello world.".getBytes();
    byte[] cipherText = encryptor.encrypt( text , 0 , text.length );

    assertEquals( cipherText.length ,
        text.length + AesGcmEncryptor.GCM_NONCE_LENGTH + AesGcmEncryptor.GCM_TAG_LENGTH );
    assertNotEquals( new String( text ) , new String( cipherText ) );
    cipherText[0] += 1;

    assertThrows( IOException.class ,
      () -> {
        encryptor.decrypt( cipherText , 0 , cipherText.length );
      }
    );
  }

}
