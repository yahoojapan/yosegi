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

import java.nio.ByteBuffer;

public class AdditionalAuthenticationData {

  private final byte[] prefix;
  private final byte[] identifier;
  private final int aadLength;

  private short blockOrdinal = 0;
  private int ordinal = 0;

  /**
   * Convert module type to byte.
   */
  public static byte typeToByte( final Module type ) {
    switch ( type ) {
      case BLOCK_META:
        return 0;
      case COLUMN_META:
        return 10;
      case COLUMN_INDEX:
        return 11;
      case COLUMN_DATA:
        return 12;
      case KEYS:
        return -1;
      default:
        throw new UnsupportedOperationException( "This type is not supported : " + type );
    }
  }

  /**
   * Convert module type to byte.
   */
  public static Module typeToByte( final byte typeByte ) {
    switch ( typeByte ) {
      case 0:
        return Module.BLOCK_META;
      case 10:
        return Module.COLUMN_META;
      case 11:
        return Module.COLUMN_INDEX;
      case 12:
        return Module.COLUMN_DATA;
      case -1:
        return Module.KEYS;
      default:
        throw new UnsupportedOperationException( "This byte is not supported : " + typeByte );
    }
  }

  /**
   * Initialize with identifier.
   */
  public AdditionalAuthenticationData( final byte[] identifier ) {
    this.identifier = new byte[identifier.length];
    System.arraycopy( identifier , 0 , this.identifier , 0 , identifier.length );
    this.prefix = new byte[0];
    aadLength = this.prefix.length
        + this.identifier.length
        + Short.BYTES
        + Byte.BYTES
        + Integer.BYTES;
  }

  /**
   * Initialize with identifier and prefix.
   */
  public AdditionalAuthenticationData( final byte[] prefix , final byte[] identifier ) {
    this.identifier = new byte[identifier.length];
    System.arraycopy( identifier , 0 , this.identifier , 0 , identifier.length );
    this.prefix = new byte[prefix.length];
    System.arraycopy( prefix , 0 , this.prefix , 0 , prefix.length );
    aadLength = this.prefix.length
        + this.identifier.length
        + Short.BYTES
        + Byte.BYTES
        + Integer.BYTES;
  }

  public byte[] getPrefix() {
    return prefix;
  }

  public byte[] getIdentifier() {
    return identifier;
  }

  public void setBlockOrdinal( final short blockOrdinal ) {
    this.blockOrdinal = blockOrdinal;
    ordinal = 0;
  }

  public void setOrdinal( final int ordinal ) {
    this.ordinal = ordinal;
  }

  public void nextBlock() {
    blockOrdinal++;
    ordinal = 0;
  }

  public void nextOrdinal() {
    ordinal++;
  }

  public short getBlockOrdinal() {
    return blockOrdinal;
  }

  public int getOrdinal() {
    return ordinal;
  }

  /**
   * Copy value to new byte array.
   */
  public byte[] create( final Module type ) {
    byte typeByte = typeToByte( type );
    ByteBuffer aad = ByteBuffer.allocate( aadLength );
    aad.put( prefix );
    aad.put( identifier );
    aad.putShort( blockOrdinal );
    aad.put( typeByte );
    aad.putInt( ordinal );
    
    return aad.array();
  }

}
