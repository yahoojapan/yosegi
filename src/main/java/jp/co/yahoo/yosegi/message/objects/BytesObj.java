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

package jp.co.yahoo.yosegi.message.objects;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class BytesObj implements PrimitiveObject {

  private byte[] value;

  public BytesObj() {
    value = new byte[0];
  }

  public BytesObj( final byte[] data ) {
    value = data;
  }

  public BytesObj( final byte[] data , final int start , final int length ) {
    value = new byte[length];
    System.arraycopy( data , start , value , 0 , length );
  }

  @Override
  public Object get() throws IOException {
    return value;
  }

  @Override
  public String getString() throws IOException {
    return new String( value , "UTF-8" );
  }

  @Override
  public byte[] getBytes() throws IOException {
    return value;
  }

  @Override
  public byte getByte() throws IOException {
    return Byte.parseByte( getString() );
  }

  @Override
  public short getShort() throws IOException {
    return Short.parseShort( getString() );
  }

  @Override
  public int getInt() throws IOException {
    return Integer.parseInt( getString() );
  }

  @Override
  public long getLong() throws IOException {
    return Long.parseLong( getString() );
  }

  @Override
  public float getFloat() throws IOException {
    return Float.parseFloat( getString() );
  }

  @Override
  public double getDouble() throws IOException {
    return Double.parseDouble( getString() );
  }

  @Override
  public boolean getBoolean() throws IOException {
    return Boolean.valueOf( getString() );
  }

  @Override
  public void setString( final String data ) throws IOException {
    value = data.getBytes( "UTF-8" );
  }

  @Override
  public void setBytes( final byte[] data ) throws IOException {
    setBytes( data , 0 , data.length );
  }

  @Override
  public void setBytes(
      final byte[] data ,
      final int start ,
      final int length ) throws IOException {
    value = new byte[ length ];
    System.arraycopy( data , start , value , 0 , length );
  }

  @Override
  public void setByte( final byte data ) throws IOException {
    setInt( (int)data );
  }

  @Override
  public void setShort( final short data ) throws IOException {
    value = Short.toString( data ).getBytes( "UTF-8" );
  }

  @Override
  public void setInt( final int data ) throws IOException {
    value = Integer.toString( data ).getBytes( "UTF-8" );
  }

  @Override
  public void setLong( final long data ) throws IOException {
    value = Long.toString( data ).getBytes( "UTF-8" );
  }

  @Override
  public void setFloat( final float data ) throws IOException {
    value = Float.toString( data ).getBytes( "UTF-8" );
  }

  @Override
  public void setDouble( final double data ) throws IOException {
    value = Double.toString( data ).getBytes( "UTF-8" );
  }

  @Override
  public void setBoolean( final boolean data ) throws IOException {
    value = Boolean.toString( data ).getBytes( "UTF-8" );
  }

  @Override
  public void set( final PrimitiveObject data ) throws IOException {
    value = data.getBytes();
  }

  @Override
  public void clear() throws IOException {
    value = new byte[0];
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.BYTES;
  }

  private void writeObject( final ObjectOutputStream out ) throws IOException {
    out.write( value.length );
    out.write( value , 0 , value.length );
  }

  private void readObject( final ObjectInputStream in ) throws IOException {
    int length = in.readInt();
    value = new byte[length];
    in.readFully( value );
  }

  @Override
  public int getObjectSize() {
    return value.length;
  }

}
