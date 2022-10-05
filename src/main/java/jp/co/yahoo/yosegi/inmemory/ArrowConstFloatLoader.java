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

package jp.co.yahoo.yosegi.inmemory;

import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.ValueVector;

import java.io.IOException;

public class ArrowConstFloatLoader implements IConstLoader<ValueVector> {

  private final Float4Vector vector;
  private final int loadSize;

  /**
   * A loader that holds elements sequentially.
   */
  public ArrowConstFloatLoader( final ValueVector vector , final int loadSize ) {
    this.vector = (Float4Vector)vector;
    this.vector.allocateNew( loadSize );
    this.vector.setValueCount( loadSize );
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public ValueVector build() throws IOException {
    return vector;
  }

  @Override
  public void finish() throws IOException {
  }

  @Override
  public void setNull( final int index ) throws IOException {}

  @Override
  public void setConstFromNull() throws IOException {
    for ( int i = 0 ; i < loadSize ; i++ ) {
      vector.setNull( i );
    }
  }

  @Override
  public void setConstFromBytes(
      final byte[] value , final int start , final int length ) throws IOException {
    setDownCastOrNull( new BytesObj( value , start , length ) );
  }

  @Override
  public void setConstFromString( final String value) throws IOException {
    setDownCastOrNull( new StringObj( value ) );
  }

  @Override
  public void setConstFromByte( final byte value ) throws IOException {
    setDownCastOrNull( new ByteObj( value ) );
  }

  @Override
  public void setConstFromShort( final short value ) throws IOException {
    setDownCastOrNull( new ShortObj( value ) );
  }

  @Override
  public void setConstFromInteger( final int value ) throws IOException {
    setDownCastOrNull( new IntegerObj( value ) );
  }

  @Override
  public void setConstFromLong( final long value ) throws IOException {
    setDownCastOrNull( new LongObj( value ) );
  }

  @Override
  public void setConstFromFloat( final float value ) throws IOException {
    for ( int i = 0 ; i < loadSize ; i++ ) {
      vector.setSafe( i , value );
    }
  }

  @Override
  public void setConstFromDouble( final double value ) throws IOException {
    setDownCastOrNull( new DoubleObj( value ) );
  }

  private void setDownCastOrNull( final PrimitiveObject obj ) throws IOException {
    try {
      setConstFromFloat( obj.getFloat() );
    } catch ( NumberFormatException ex ) {
      setConstFromNull();
    }
  }

}
