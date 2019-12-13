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

import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.AesGcmEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.EncryptionKey;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.Arrays;

public class TestKeyStore {

  @Test
  public void T_createBinaryAndRegistFromBinary_equalsSetValue() throws IOException {
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( identifier );
    AesGcmEncryptorFactory factory = new AesGcmEncryptorFactory();

    EncryptionKey key1 = new EncryptionKey( "sercretkey128_01".getBytes() ); 
    EncryptionKey key2 = new EncryptionKey( "sercretkey128_02".getBytes() ); 
    EncryptionKey key3 = new EncryptionKey( "sercretkey128_03".getBytes() ); 
    EncryptionKey key4 = new EncryptionKey( "sercretkey128_04".getBytes() ); 
    EncryptionKey key5 = new EncryptionKey( "sercretkey128_05".getBytes() ); 

    KeyStore keyStore = new KeyStore();
    keyStore.registKey( "one" , key1 );
    keyStore.registKey( "two" , key2 );
    keyStore.registKey( "three" , key3 );
    keyStore.registKey( "four" , key4 );
    keyStore.registKey( "five" , key5 );

    aad.setOrdinal(0);
    byte[] keyMappingBinary = keyStore.createCheckKeyBinary( aad , factory );

    KeyStore newKeyStore = new KeyStore();
    EncryptionKey[] keys = new EncryptionKey[]{ key1 , key2 , key3 , key4 , key5 };
    aad.setOrdinal(0);
    newKeyStore.registKeyFromBinary( keys , aad , factory , keyMappingBinary , 0 , keyMappingBinary.length );

    assertTrue( newKeyStore.contains( "one" ) );
    assertTrue( newKeyStore.contains( "two" ) );
    assertTrue( newKeyStore.contains( "three" ) );
    assertTrue( newKeyStore.contains( "four" ) );
    assertTrue( newKeyStore.contains( "five" ) );

    assertTrue( Arrays.equals( newKeyStore.getKey( "one" ).getKey() , keyStore.getKey( "one" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "two" ).getKey() , keyStore.getKey( "two" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "three" ).getKey() , keyStore.getKey( "three" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "four" ).getKey() , keyStore.getKey( "four" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "five" ).getKey() , keyStore.getKey( "five" ).getKey() ) );
  }

  @Test
  public void T_createBinaryAndRegistFromBinary_equalsSetValue_whenUseSomeKey() throws IOException {
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( identifier );
    AesGcmEncryptorFactory factory = new AesGcmEncryptorFactory();

    EncryptionKey key1 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key2 = new EncryptionKey( "sercretkey128_02".getBytes() );
    EncryptionKey key3 = new EncryptionKey( "sercretkey128_03".getBytes() );
    EncryptionKey key4 = new EncryptionKey( "sercretkey128_04".getBytes() );
    EncryptionKey key5 = new EncryptionKey( "sercretkey128_05".getBytes() );

    KeyStore keyStore = new KeyStore();
    keyStore.registKey( "one" , key1 );
    keyStore.registKey( "two" , key2 );
    keyStore.registKey( "three" , key3 );
    keyStore.registKey( "four" , key4 );
    keyStore.registKey( "five" , key5 );

    aad.setOrdinal(0);
    byte[] keyMappingBinary = keyStore.createCheckKeyBinary( aad , factory );

    KeyStore newKeyStore = new KeyStore();
    EncryptionKey[] keys = new EncryptionKey[]{ key2 , key4 };
    aad.setOrdinal(0);
    newKeyStore.registKeyFromBinary( keys , aad , factory , keyMappingBinary , 0 , keyMappingBinary.length );

    assertFalse( newKeyStore.contains( "one" ) );
    assertTrue( newKeyStore.contains( "two" ) );
    assertFalse( newKeyStore.contains( "three" ) );
    assertTrue( newKeyStore.contains( "four" ) );
    assertFalse( newKeyStore.contains( "five" ) );

    assertTrue( Arrays.equals( newKeyStore.getKey( "two" ).getKey() , keyStore.getKey( "two" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "four" ).getKey() , keyStore.getKey( "four" ).getKey() ) );
  }

  @Test
  public void T_createBinaryAndRegistFromBinary_equalsSetValue_whenUseSameKey() throws IOException {
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( identifier );
    AesGcmEncryptorFactory factory = new AesGcmEncryptorFactory();

    EncryptionKey key1 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key2 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key3 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key4 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key5 = new EncryptionKey( "sercretkey128_01".getBytes() );

    KeyStore keyStore = new KeyStore();
    keyStore.registKey( "one" , key1 );
    keyStore.registKey( "two" , key2 );
    keyStore.registKey( "three" , key3 );
    keyStore.registKey( "four" , key4 );
    keyStore.registKey( "five" , key5 );

    aad.setOrdinal(0);
    byte[] keyMappingBinary = keyStore.createCheckKeyBinary( aad , factory );

    KeyStore newKeyStore = new KeyStore();
    EncryptionKey[] keys = new EncryptionKey[]{ key1 , key2 , key3 , key4 , key5 };
    aad.setOrdinal(0);
    newKeyStore.registKeyFromBinary( keys , aad , factory , keyMappingBinary , 0 , keyMappingBinary.length );

    assertTrue( newKeyStore.contains( "one" ) );
    assertTrue( newKeyStore.contains( "two" ) );
    assertTrue( newKeyStore.contains( "three" ) );
    assertTrue( newKeyStore.contains( "four" ) );
    assertTrue( newKeyStore.contains( "five" ) );

    assertTrue( Arrays.equals( newKeyStore.getKey( "one" ).getKey() , keyStore.getKey( "one" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "two" ).getKey() , keyStore.getKey( "two" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "three" ).getKey() , keyStore.getKey( "three" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "four" ).getKey() , keyStore.getKey( "four" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "five" ).getKey() , keyStore.getKey( "five" ).getKey() ) );
  }

  @Test
  public void T_createBinaryAndRegistFromBinary_equalsSetValue_whenUseSameKeyAndSomeKey() throws IOException {
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( identifier );
    AesGcmEncryptorFactory factory = new AesGcmEncryptorFactory();

    EncryptionKey key1 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key2 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key3 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key4 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key5 = new EncryptionKey( "sercretkey128_01".getBytes() );

    KeyStore keyStore = new KeyStore();
    keyStore.registKey( "one" , key1 );
    keyStore.registKey( "two" , key2 );
    keyStore.registKey( "three" , key3 );
    keyStore.registKey( "four" , key4 );
    keyStore.registKey( "five" , key5 );

    aad.setOrdinal(0);
    byte[] keyMappingBinary = keyStore.createCheckKeyBinary( aad , factory );

    KeyStore newKeyStore = new KeyStore();
    EncryptionKey[] keys = new EncryptionKey[]{ key1 };
    aad.setOrdinal(0);
    newKeyStore.registKeyFromBinary( keys , aad , factory , keyMappingBinary , 0 , keyMappingBinary.length );

    assertTrue( newKeyStore.contains( "one" ) );
    assertTrue( newKeyStore.contains( "two" ) );
    assertTrue( newKeyStore.contains( "three" ) );
    assertTrue( newKeyStore.contains( "four" ) );
    assertTrue( newKeyStore.contains( "five" ) );

    assertTrue( Arrays.equals( newKeyStore.getKey( "one" ).getKey() , keyStore.getKey( "one" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "two" ).getKey() , keyStore.getKey( "two" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "three" ).getKey() , keyStore.getKey( "three" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "four" ).getKey() , keyStore.getKey( "four" ).getKey() ) );
    assertTrue( Arrays.equals( newKeyStore.getKey( "five" ).getKey() , keyStore.getKey( "five" ).getKey() ) );
  }

  @Test
  public void T_createBinaryAndRegistFromBinary_equalsSetValue_whenNoMatch() throws IOException {
    byte[] identifier = "This is test.".getBytes();
    AdditionalAuthenticationData aad = new AdditionalAuthenticationData( identifier );
    AesGcmEncryptorFactory factory = new AesGcmEncryptorFactory();

    EncryptionKey key1 = new EncryptionKey( "sercretkey128_01".getBytes() );
    EncryptionKey key2 = new EncryptionKey( "sercretkey128_02".getBytes() );
    EncryptionKey key3 = new EncryptionKey( "sercretkey128_03".getBytes() );
    EncryptionKey key4 = new EncryptionKey( "sercretkey128_04".getBytes() );
    EncryptionKey key5 = new EncryptionKey( "sercretkey128_05".getBytes() );
    EncryptionKey key6 = new EncryptionKey( "sercretkey128_06".getBytes() );

    KeyStore keyStore = new KeyStore();
    keyStore.registKey( "one" , key1 );
    keyStore.registKey( "two" , key2 );
    keyStore.registKey( "three" , key3 );
    keyStore.registKey( "four" , key4 );
    keyStore.registKey( "five" , key5 );

    aad.setOrdinal(0);
    byte[] keyMappingBinary = keyStore.createCheckKeyBinary( aad , factory );

    KeyStore newKeyStore = new KeyStore();
    EncryptionKey[] keys = new EncryptionKey[]{ key6 };
    aad.setOrdinal(0);
    newKeyStore.registKeyFromBinary( keys , aad , factory , keyMappingBinary , 0 , keyMappingBinary.length );
    assertEquals( 0 , newKeyStore.size() );
  }

}
