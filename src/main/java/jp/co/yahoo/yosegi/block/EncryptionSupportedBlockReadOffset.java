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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.encryptor.AdditionalAuthenticationData;
import jp.co.yahoo.yosegi.encryptor.EncryptionKey;
import jp.co.yahoo.yosegi.encryptor.FindEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.Module;
import jp.co.yahoo.yosegi.keystore.KeyStore;
import jp.co.yahoo.yosegi.util.io.InputStreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class EncryptionSupportedBlockReadOffset
    implements Comparable<EncryptionSupportedBlockReadOffset> {

  public final int streamStart;
  public final int encryptBinaryLength;
  public final int bufferStart;
  public final byte[] buffer;
  public final boolean isEncrypt;
  public final String keyName;

  /**
   * Set byte array of blocks.
   */
  public EncryptionSupportedBlockReadOffset(
      final int streamStart ,
      final int encryptBinaryLength ,
      final int bufferStart ,
      final byte[] buffer ,
      final boolean isEncrypt ,
      final String keyName ) {
    this.streamStart = streamStart;
    this.encryptBinaryLength = encryptBinaryLength;
    this.bufferStart = bufferStart;
    this.buffer = buffer;
    this.isEncrypt = isEncrypt;
    this.keyName = keyName;
  }

  @Override
  public int compareTo( final EncryptionSupportedBlockReadOffset target ) {
    if ( this.streamStart > target.streamStart ) {
      return 1;
    } else if ( this.streamStart < target.streamStart ) {
      return -1;
    }

    return 0;
  }

  /**
   * Read data into buffer.
   */
  public int read(
      final InputStream in ,
      final AdditionalAuthenticationData aad,
      final KeyStore keyStore,
      final IEncryptorFactory factory ) throws IOException {
    if ( ! isEncrypt ) {
      InputStreamUtils.read( in , buffer , bufferStart , encryptBinaryLength );
      return encryptBinaryLength;
    }
    byte[] encryptBinary = new byte[encryptBinaryLength];
    InputStreamUtils.read( in , encryptBinary , 0 , encryptBinary.length );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( encryptBinary );

    aad.setOrdinal( wrapBuffer.getInt() );
    int length = wrapBuffer.getInt();
    IEncryptor encryptor = factory.createEncryptor(
        keyStore.getKey( keyName ),
        Module.COLUMN_DATA,
        aad );
    byte[] dataBinary = encryptor.decrypt( encryptBinary , Integer.BYTES * 2 , length );
    ByteBuffer.wrap( buffer , bufferStart , dataBinary.length ).put( dataBinary );

    return encryptBinaryLength;
  }

  public int getEncryptBinaryLength() {
    return encryptBinaryLength;
  }

}
