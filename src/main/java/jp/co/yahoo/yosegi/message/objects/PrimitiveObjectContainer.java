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

import java.util.HashMap;
import java.util.Map;

public class PrimitiveObjectContainer {

  private final Map<String,PrimitiveObject> primitiveContainer =
      new HashMap<String,PrimitiveObject>();

  /**
   * Register PrimitiveObject from class name with the specified variable name.
   */
  public void register( final String varName , final String className ) throws IOException {
    PrimitiveObject primitiveObject;
    try {
      primitiveObject = PrimitiveObjectMaker.create( className );
    } catch ( ClassNotFoundException ex ) {
      throw new IOException( ex );
    } catch ( InstantiationException ex ) {
      throw new IOException( ex );
    } catch ( IllegalAccessException ex ) {
      throw new IOException( ex );
    }
    register( varName , primitiveObject );
  }

  /**
   * Register PrimitiveObject with the specified variable name.
   */
  public void register(
      final String varName ,
      final PrimitiveObject primitiveObject ) throws IOException {
    if ( primitiveContainer.containsKey( varName ) ) {
      throw new IOException( String.format( "%s is alraedy register." , varName ) );
    }
    primitiveContainer.put( varName , primitiveObject );
  }

  public boolean contains( final String varName ) {
    return primitiveContainer.containsKey( varName );
  }

  public PrimitiveObject get( final String varName ) {
    return primitiveContainer.get( varName );
  }

  /**
   * Clear this object.
   */
  public void reset() throws IOException {
    for ( Map.Entry<String,PrimitiveObject> entry : primitiveContainer.entrySet() ) {
      entry.getValue().clear();
    }
  }

  public void clear() {
    primitiveContainer.clear();
  }

  public void set( final String varName , final String data ) throws IOException {
    primitiveContainer.get( varName ).setString( data );
  }

  public void set( final String varName , final byte[] data ) throws IOException {
    primitiveContainer.get( varName ).setBytes( data );
  }

  public void set(
      final String varName ,
      final byte[] data ,
      final int start ,
      final int length ) throws IOException {
    primitiveContainer.get( varName ).setBytes( data , start , length );
  }

  public void set( final String varName , final short data ) throws IOException {
    primitiveContainer.get( varName ).setShort( data );
  }

  public void set( final String varName , final int data ) throws IOException {
    primitiveContainer.get( varName ).setInt( data );
  }

  public void set( final String varName , final long data ) throws IOException {
    primitiveContainer.get( varName ).setLong( data );
  }

  public void set( final String varName , final float data ) throws IOException {
    primitiveContainer.get( varName ).setFloat( data );
  }

  public void set( final String varName , final double data ) throws IOException {
    primitiveContainer.get( varName ).setDouble( data );
  }

  public void set( final String varName , final boolean data ) throws IOException {
    primitiveContainer.get( varName ).setBoolean( data );
  }

  public void set( final String varName , final PrimitiveObject data ) throws IOException {
    primitiveContainer.get( varName ).set( data );
  }

}
