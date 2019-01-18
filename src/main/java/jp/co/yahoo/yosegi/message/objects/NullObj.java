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

public class NullObj implements PrimitiveObject {

  private static final NullObj NULL_OBJ = new NullObj();

  private NullObj() {}

  public static PrimitiveObject getInstance() {
    return NULL_OBJ;
  }

  @Override
  public Object get() throws IOException {
    return null;
  }

  @Override
  public String getString() throws IOException {
    return null;
  }

  @Override
  public byte[] getBytes() throws IOException {
    return null;
  }

  @Override
  public byte getByte() throws IOException {
    throw new NumberFormatException( "NullObj is not support getByte()" );
  }

  @Override
  public short getShort() throws IOException {
    throw new NumberFormatException( "NullObj is not support getShort()" );
  }

  @Override
  public int getInt() throws IOException {
    throw new NumberFormatException( "NullObj is not support getInt()" );
  }

  @Override
  public long getLong() throws IOException {
    throw new NumberFormatException( "NullObj is not support getLong()" );
  }

  @Override
  public float getFloat() throws IOException {
    throw new NumberFormatException( "NullObj is not support getFloat()" );
  }

  @Override
  public double getDouble() throws IOException {
    throw new NumberFormatException( "NullObj is not support getDouble()" );
  }

  @Override
  public boolean getBoolean() throws IOException {
    throw new NullPointerException( "NullObj is not support getBoolean()" );
  }

  @Override
  public void setString( final String data ) throws IOException {}

  @Override
  public void setBytes( final byte[] data ) throws IOException {}

  @Override
  public void setBytes(
      final byte[] data ,
      final int start ,
      final int length ) throws IOException {}

  @Override
  public void setByte( final byte data ) throws IOException {}

  @Override
  public void setShort( final short data ) throws IOException {}

  @Override
  public void setInt( final int data ) throws IOException {}

  @Override
  public void setLong( final long data ) throws IOException {}

  @Override
  public void setFloat( final float data ) throws IOException {}

  @Override
  public void setDouble( final double data ) throws IOException {}

  @Override
  public void setBoolean( final boolean data ) throws IOException {}

  @Override
  public void set( final PrimitiveObject data ) throws IOException {}

  @Override
  public void clear() throws IOException {}

  @Override
  public PrimitiveType getPrimitiveType() {
    return PrimitiveType.NULL;
  }

  @Override
  public int getObjectSize() {
    return 0;
  }

}
