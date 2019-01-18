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

package jp.co.yahoo.yosegi.util;

public class ByteArrayData {

  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final int UP_DATA_SIZE_INTERVAL = 1024 * 1024;

  private byte[] data;
  private int length;

  public ByteArrayData() {
    data = new byte[ DEFAULT_BUFFER_SIZE ];
    length = 0;
  }

  public ByteArrayData( final int bufferSize ) {
    data = new byte[bufferSize];
    length = 0;
  }

  public void clear() {
    length = 0;
  }

  public int getLength() {
    return length;
  }

  public byte[] getBytes() {
    return data;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    for ( int i = 0; i < length; i++ ) {
      hash = (31 * hash) + (int)data[i];
    }
    return hash;
  }

  @Override
  public boolean equals( final Object obj ) {
    if ( ! ( obj instanceof ByteArrayData ) ) {
      return false;
    }
    ByteArrayData target = (ByteArrayData)obj;
    if ( getLength() != target.getLength() ) {
      return false;
    }
    byte[] binary = getBytes();
    byte[] targetBinary = target.getBytes();
    for ( int i = 0 ; i < getLength() ; i++ ) {
      if ( binary[i] != targetBinary[i] ) {
        return false;
      }
    }
    return true;
  }

  /**
   * Add byte array from other ByteArrayData object.
   */
  public void append( final ByteArrayData target ) {
    if ( target == null ) {
      return;
    }

    append( target.getBytes() , 0 , target.getLength() );
  }

  /**
   * Add byte.
   */
  public void append( final byte targetByte ) {
    checkSize( 1 );
    data[length] = targetByte;
    length += 1;
  }

  /**
   * Add byte array.
   */
  public void append( final byte[] targetBytes ) {
    append( targetBytes , 0 , targetBytes.length );
  }

  /**
   * Add byte array.
   */
  public void append( final byte[] targetBytes , final int targetStart , final int targetLength ) {
    checkSize( targetLength );
    System.arraycopy( targetBytes , targetStart , data , length , targetLength );
    length += targetLength;
  }

  private void checkSize( final int addLength ) {
    if ( data.length < ( length + addLength ) ) {
      int newDataSize = data.length + UP_DATA_SIZE_INTERVAL;
      while ( newDataSize < ( length + addLength ) ) {
        newDataSize += UP_DATA_SIZE_INTERVAL;
      }
      byte[] newBytes = new byte[ newDataSize ];
      System.arraycopy( data , 0 , newBytes , 0 , length );
      data = newBytes;
    }
  }

}
