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

public class StringObj implements PrimitiveObject {

  private static final String EMPTY_STRING = new String();

  private String value;

  public StringObj() {
    value = EMPTY_STRING;
  }

  public StringObj( final String value ) {
    this.value = value;
  }

  @Override
  public Object get() throws IOException {
    return value;
  }

  @Override
  public String getString() throws IOException {
    return value;
  }

  @Override
  public byte[] getBytes() throws IOException {
    if ( value == null ) {
      return null;
    }
    return value.getBytes( "UTF-8" );
  }

  @Override
  public byte getByte() throws IOException {
    return '\0';
  }

  @Override
  public short getShort() throws IOException {
    return Short.parseShort( value );
  }

  @Override
  public int getInt() throws IOException {
    return Integer.parseInt( value );
  }

  @Override
  public long getLong() throws IOException {
    return Long.parseLong( value );
  }

  @Override
  public float getFloat() throws IOException {
    return Float.parseFloat( value );
  }

  @Override
  public double getDouble() throws IOException {
    return Double.parseDouble( value );
  }

  @Override
  public boolean getBoolean() throws IOException {
    return Boolean.valueOf( value );
  }

  @Override
  public void setString( final String data ) throws IOException {
    value = data;
  }

  @Override
  public void setBytes( final byte[] data ) throws IOException {
    value = new String( data , "UTF-8");
  }

  @Override
  public void setBytes(
      final byte[] data ,
      final int start ,
      final int length ) throws IOException {
    value = new String( data , start , length , "UTF-8" );
  }

  @Override
  public void setByte( final byte data ) throws IOException {
    setInt( (int)data );
  }

  @Override
  public void setShort( final short data ) throws IOException {
    value = Short.valueOf( data ).toString();
  }

  @Override
  public void setInt( final int data ) throws IOException {
    value = Integer.valueOf( data ).toString();
  }

  @Override
  public void setLong( final long data ) throws IOException {
    value = Long.valueOf( data ).toString();
  }

  @Override
  public void setFloat( final float data ) throws IOException {
    value = Float.valueOf( data ).toString();
  }

  @Override
  public void setDouble( final double data ) throws IOException {
    value = Double.valueOf( data ).toString();
  }

  @Override
  public void setBoolean( final boolean data ) throws IOException {
    value = Boolean.toString( data );
  }

  @Override
  public void set( final PrimitiveObject data ) throws IOException {
    value = data.getString();
  }

  @Override
  public void clear() throws IOException {
    value = EMPTY_STRING;
  }

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.STRING;
  }

  @Override
  public int getObjectSize() {
    return value.length() * Character.BYTES;
  }

}
