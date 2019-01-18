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
import java.io.Serializable;

public interface PrimitiveObject extends Serializable {

  Object get() throws IOException;

  String getString() throws IOException;

  byte[] getBytes() throws IOException;

  byte getByte() throws IOException;

  short getShort() throws IOException;

  int getInt() throws IOException;

  long getLong() throws IOException;

  float getFloat() throws IOException;

  double getDouble() throws IOException;

  boolean getBoolean() throws IOException;

  void setString( final String data ) throws IOException;

  void setBytes( final byte[] data ) throws IOException;

  void setBytes( final byte[] data , final int start , final int length ) throws IOException;

  void setByte( final byte data ) throws IOException;

  void setShort( final short data ) throws IOException;

  void setInt( final int data ) throws IOException;

  void setLong( final long data ) throws IOException;

  void setFloat( final float data ) throws IOException;

  void setDouble( final double data ) throws IOException;

  void setBoolean( final boolean data ) throws IOException;

  void set( final PrimitiveObject data ) throws IOException;

  void clear() throws IOException;

  PrimitiveType getPrimitiveType();

  int getObjectSize();

}
