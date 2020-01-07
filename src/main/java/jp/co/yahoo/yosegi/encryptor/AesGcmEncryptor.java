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

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AesGcmEncryptor implements IEncryptor {

  public static final int GCM_NONCE_LENGTH = 12;
  public static final int GCM_TAG_LENGTH = 16;
  public static final SecureRandom rnd = new SecureRandom();

  private final EncryptionKey key;
  private final byte[] aad;

  public AesGcmEncryptor(
      final EncryptionKey key , final byte[] aad ) {
    this.key = key;
    this.aad = aad;
  }

  @Override
  public byte[] encrypt(
      final byte[] data , final int start , final int length ) throws IOException {
    try {
      byte[] nonce = new byte[GCM_NONCE_LENGTH];
      rnd.nextBytes( nonce );
      GCMParameterSpec spec = new GCMParameterSpec( GCM_TAG_LENGTH * 8 , nonce );

      SecretKeySpec secretKey = new SecretKeySpec( key.getKey() , "AES" );
      Cipher cipher = Cipher.getInstance( "AES/GCM/NoPadding" );
      cipher.init( Cipher.ENCRYPT_MODE , secretKey , spec );
      cipher.updateAAD( aad );

      byte[] cipherText = cipher.doFinal( data , start , length );
      byte[] result = new byte[ GCM_NONCE_LENGTH + cipherText.length ];
      System.arraycopy( nonce , 0 , result , 0 , GCM_NONCE_LENGTH );
      System.arraycopy( cipherText , 0 , result , GCM_NONCE_LENGTH , cipherText.length );

      return result;
    } catch ( Exception ex ) {
      throw new IOException( ex );
    }
  }

  @Override
  public byte[] decrypt(
      final byte[] data , final int start , final int length ) throws IOException {
    try {
      GCMParameterSpec spec = new GCMParameterSpec(
          GCM_TAG_LENGTH * 8 , data , start , GCM_NONCE_LENGTH );

      SecretKeySpec secretKey = new SecretKeySpec( key.getKey() , "AES" );
      Cipher cipher = Cipher.getInstance( "AES/GCM/NoPadding" );
      cipher.init( Cipher.DECRYPT_MODE , secretKey , spec );
      cipher.updateAAD( aad );
      return cipher.doFinal( data , start + GCM_NONCE_LENGTH , length - GCM_NONCE_LENGTH );
    } catch ( Exception ex ) {
      throw new IOException( ex );
    }
  }

}
