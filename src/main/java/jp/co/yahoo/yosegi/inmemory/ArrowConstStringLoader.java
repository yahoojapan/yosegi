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

import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.ByteObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.ShortObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

import java.io.IOException;

public class ArrowConstStringLoader implements IConstLoader<ValueVector> {

  private final VarCharVector vector;
  private final int loadSize;

  /**
   * A loader that holds elements sequentially.
   */
  public ArrowConstStringLoader( final ValueVector vector , final int loadSize ) {
    this.vector = (VarCharVector)vector;
    this.vector.allocateNew();
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
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    for ( int i = 0 ; i < loadSize ; i++ ) {
      vector.setSafe( i , value , start , length );
    }
  }

  @Override
  public void setConstFromString( final String value ) throws IOException {
    setConstFromBytes( new StringObj( value ).getBytes() );
  }

  @Override
  public void setConstFromBoolean(  final boolean value ) throws IOException {
    setConstFromBytes( new BooleanObj( value ).getBytes() );
  }

  @Override
  public void setConstFromByte(  final byte value ) throws IOException {
    setConstFromBytes( new ByteObj( value ).getBytes() );
  }

  @Override
  public void setConstFromShort(  final short value ) throws IOException {
    setConstFromBytes( new ShortObj( value ).getBytes() );
  }

  @Override
  public void setConstFromInteger(  final int value ) throws IOException {
    setConstFromBytes( new IntegerObj( value ).getBytes() );
  }

  @Override
  public void setConstFromLong(  final long value ) throws IOException {
    setConstFromBytes( new LongObj( value ).getBytes() );
  }

  @Override
  public void setConstFromFloat(  final float value ) throws IOException {
    setConstFromBytes( new FloatObj( value ).getBytes() );
  }

  @Override
  public void setConstFromDouble(  final double value ) throws IOException {
    setConstFromBytes( new DoubleObj( value ).getBytes() );
  }

}