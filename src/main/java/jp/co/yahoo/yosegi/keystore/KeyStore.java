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
import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.EncryptionKey;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.Module;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KeyStore {

  private final Map<String,EncryptionKey> keyStore;

  public KeyStore() {
    keyStore = new HashMap<String,EncryptionKey>();
  }

  public int size() {
    return keyStore.size();
  }

  public boolean contains( final String name ) {
    return keyStore.containsKey( name );
  }

  public EncryptionKey getKey( final String name ) {
    return keyStore.get( name );
  }

  public void registKey( final String name , final EncryptionKey key ) throws IOException {
    keyStore.put( name , key );
  }

  public Set<String> getKeyNameSet() {
    return keyStore.keySet();
  }

  private void checkAndRegistKey(
      final EncryptionKey[] keys,
      final AdditionalAuthenticationData aad ,
      final IEncryptorFactory factory,
      final byte[] encryptData ) throws IOException {
    for ( int i = 0 ; i < keys.length ; i++ ) {
      IEncryptor encryptor = factory.createEncryptor( keys[i] , Module.KEYS , aad );
      try {
        byte[] nameBinary = encryptor.decrypt( encryptData , 0 , encryptData.length );
        registKey( new String( nameBinary ) , keys[i] );
      } catch ( IOException ex ) {
        continue;
      }
    }
  }

  /**
   * Create a mapping between key and internal name.
   */
  public void registKeyFromBinary(
      final EncryptionKey[] keys,
      final AdditionalAuthenticationData aad ,
      final IEncryptorFactory factory,
      final byte[] binary ,
      final int start ,
      final int length ) throws IOException {
    ByteBuffer wrapBuffer = ByteBuffer.wrap( binary , start , length );
    int keySize = wrapBuffer.getInt();
    for ( int i = 0 ; i < keySize ; i++ ) {
      byte[] encryptData = new byte[wrapBuffer.getInt()];
      wrapBuffer.get( encryptData );
      checkAndRegistKey(
          keys , aad , factory , encryptData );
      aad.nextOrdinal();
    }
  }

  /**
   * Get check key binary size.
   */
  public int createCheckKeyBinarySize(
      final AdditionalAuthenticationData aad ,
      final IEncryptorFactory factory ) throws IOException {
    int keyBinaryLength = Integer.BYTES;
    for ( Map.Entry<String,EncryptionKey> entry : keyStore.entrySet() ) {
      byte[] nameBinary = entry.getKey().getBytes();
      keyBinaryLength += Integer.BYTES
          + factory.getEncryptSize( nameBinary.length , Module.KEYS , aad );
      aad.nextOrdinal();
    }
    return keyBinaryLength;
  }

  /**
   * Create a binary to identify the key ID used in the write process during the read process.
   */
  public byte[] createCheckKeyBinary(
      final AdditionalAuthenticationData aad ,
      final IEncryptorFactory factory ) throws IOException {
    byte[][] checkKeyBinary = new byte[keyStore.size()][];
    int index = 0;
    int length = 0;
    for ( Map.Entry<String,EncryptionKey> entry : keyStore.entrySet() ) {
      IEncryptor encryptor = factory.createEncryptor( entry.getValue() , Module.KEYS , aad );
      byte[] nameBinary = entry.getKey().getBytes();
      byte[] encryptData = encryptor.encrypt( nameBinary , 0 , nameBinary.length );
      checkKeyBinary[index] = new byte[ Integer.BYTES + encryptData.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( checkKeyBinary[index] );
      wrapBuffer.putInt( encryptData.length );
      wrapBuffer.put( encryptData );
      length += checkKeyBinary[index].length;
      index++;
      aad.nextOrdinal();
    }
    byte[] result = new byte[ Integer.BYTES + length];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( result );
    wrapBuffer.putInt( checkKeyBinary.length );
    for ( int i = 0 ; i < checkKeyBinary.length ; i++ ) {
      wrapBuffer.put( checkKeyBinary[i] );
    }
    return result;
  }

  public static KeyStore createFromJson( final String json ) 
      throws IOException {
    return createFromParser( new JacksonMessageReader().create( json ) );
  }

  /**
   * Create a keystore from IParser.
   */
  public static KeyStore createFromParser( final IParser parser ) 
      throws IOException {
    KeyStore keyStore = new KeyStore();
    for ( int i = 0 ; i < parser.size() ; i++ ) {
      IParser childParser = parser.getParser(i);
      if ( ! childParser.containsKey( "name" ) ) {
        throw new IOException(
            "There is no key name. Enter a value for \"name\"." );
      }
      String keyName = childParser.get( "name" ).getString();
      keyStore.registKey(
          keyName , createEncryptionKeyFromParser( childParser ) );
    }
    return keyStore;
  }

  public static EncryptionKey[] createKeysFromJson( final String json ) throws IOException {
    return createKeysFromParser( new JacksonMessageReader().create( json ) );
  }

  /**
   * Create a keys from IParser.
   */
  public static EncryptionKey[] createKeysFromParser( final IParser parser ) throws IOException {
    String[] keyNames = parser.getAllKey();
    EncryptionKey[] keys = new EncryptionKey[parser.size()];
    for ( int i = 0 ; i < parser.size() ; i++ ) {
      IParser childParser = parser.getParser(i);
      keys[i] = createEncryptionKeyFromParser( childParser );
    }
    return keys;
  }

  private static EncryptionKey createEncryptionKeyFromParser(
      final IParser parser ) throws IOException {
    if ( ! parser.containsKey( "class" ) ) {
      throw new IOException(
          "There is no key provider class name. Enter a value for \"class\"." );
    }
    IKeyProvider keyProvider = FindKeyProvider.get( parser.get( "class" ).getString() ); 
    if ( parser.containsKey( "config" ) ) {
      IParser configParser = parser.getParser( "config" );
      Configuration config = new Configuration();
      String[] configNames = configParser.getAllKey();
      for ( String configName : configNames ) {
        config.set( configName , configParser.get( configName ).getString() );
      }
      keyProvider.setup( config );
    }
    return keyProvider.getKey();
  }

}
