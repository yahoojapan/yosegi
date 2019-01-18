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

import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;

public class ArrowFixedSchemaMapMemoryAllocator implements IMemoryAllocator {

  private final IField childSchema;
  private final StructVector vector;
  private final BufferAllocator allocator;
  private final int rowCount;

  /**
   * Set the vector of Map and initialize it.
   */
  public ArrowFixedSchemaMapMemoryAllocator(
      final MapContainerField schema ,
      final BufferAllocator allocator ,
      final StructVector vector , final int rowCount ) {
    this.allocator = allocator;
    this.vector = vector;
    this.rowCount = rowCount;
    vector.allocateNew();
    childSchema = schema.getField();
  }

  @Override
  public void setNull( final int index ) {
  }

  @Override
  public void setBoolean( final int index , final boolean value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setBoolean()" );
  }

  @Override
  public void setByte( final int index , final byte value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setByte()" );
  }

  @Override
  public void setShort( final int index , final short value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setShort()" );
  }

  @Override
  public void setInteger( final int index , final int value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setInteger()" );
  }

  @Override
  public void setLong( final int index , final long value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setLong()" );
  }

  @Override
  public void setFloat( final int index , final float value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setFloat()" );
  }

  @Override
  public void setDouble( final int index , final double value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setDouble()" );
  }

  @Override
  public void setBytes( final int index , final byte[] value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setBytes()" );
  }

  @Override
  public void setBytes(
      final int index ,
      final byte[] value ,
      final int start ,
      final int length ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setBytes()" );
  }

  @Override
  public void setString( final int index , final String value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setString( final int index , final char[] value ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setString(
      final int index ,
      final char[] value ,
      final int start ,
      final int length ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setString()" );
  }

  @Override
  public void setArrayIndex(
      final int index , final int start , final int length ) throws IOException {
    throw new UnsupportedOperationException( "Unsupported method setArrayIndex()" );
  }

  @Override
  public void setValueCount( final int count ) throws IOException {
    for ( int i = 0 ; i < count ; i++ ) {
      vector.setIndexDefined(i);
    }
    vector.setValueCount( count );
  }

  @Override
  public int getValueCount() throws IOException {
    return vector.getValueCount();
  }

  @Override
  public IMemoryAllocator getChild(
      final String columnName , final ColumnType type ) throws IOException {
    return ArrowFixedSchemaMemoryAllocatorFactory.getFromStructVector(
        childSchema , columnName , allocator , vector , rowCount );
  }

}
