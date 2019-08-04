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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Utf8BytesLinkObj extends StringObj implements IBytesLink {

  private int start;
  private int length;
  private byte[] value;

  /**
   * Creates a PrimitiveObject of String referencing the input byte array.
   */
  public Utf8BytesLinkObj( final byte[] data , final int start , final int length ) {
    this.value = data;
    this.start = start;
    this.length = length;
  }

  @Override
  public byte[] getLinkBytes() {
    return value;
  }

  @Override
  public int getStart() {
    return start;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public Object get() throws IOException {
    throw new IOException( "Unsupported get object method." );
  }

  @Override
  public String getString() throws IOException {
    return new String( value , start , length , "UTF-8" );
  }

  @Override
  public byte[] getBytes() throws IOException {
    byte[] result = new byte[length];
    System.arraycopy( value , start , result , 0 , length );
    return result;
  }

  @Override
  public int getObjectSize() {
    return length;
  }

  private void writeObject( final ObjectOutputStream out ) throws IOException {
    out.write( length );
    out.write( value , start , length );
  }

  private void readObject( final ObjectInputStream in ) throws IOException {
    start = 0;
    length = in.readInt();
    value = new byte[length];
    in.readFully( value );
  }

}
