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
import jp.co.yahoo.yosegi.message.objects.BytesObj;
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

public class ArrowSequentialStringLoader implements ISequentialLoader<ValueVector> {

  private final VarCharVector vector;
  private final int loadSize;

  /**
   * A loader that holds elements sequentially.
   */
  public ArrowSequentialStringLoader( final ValueVector vector , final int loadSize ) {
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
  public void setNull( final int index ) throws IOException {
    vector.setNull( index );
  }

  @Override
  public void setBytes(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    vector.setSafe( index , value , start , length );
  }

  @Override
  public void setString( final int index , final String value ) throws IOException {
    setBytes( index , new StringObj( value ).getBytes() );
  }

  @Override
  public void setBoolean( final int index , final boolean value ) throws IOException {
    setBytes( index , new BooleanObj( value ).getBytes() );
  }

  @Override
  public void setByte( final int index , final byte value ) throws IOException {
    setBytes( index , new ByteObj( value ).getBytes() );
  }

  @Override
  public void setShort( final int index , final short value ) throws IOException {
    setBytes( index , new ShortObj( value ).getBytes() );
  }

  @Override
  public void setInteger( final int index , final int value ) throws IOException {
    setBytes( index , new IntegerObj( value ).getBytes() );
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException {
    setBytes( index , new LongObj( value ).getBytes() );
  }

  @Override
  public void setFloat( final int index , final float value ) throws IOException {
    setBytes( index , new FloatObj( value ).getBytes() );
  }

  @Override
  public void setDouble( final int index , final double value ) throws IOException {
    setBytes( index , new DoubleObj( value ).getBytes() );
  }

}
