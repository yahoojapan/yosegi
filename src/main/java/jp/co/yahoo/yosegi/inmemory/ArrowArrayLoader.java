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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.IField;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;

import java.io.IOException;

public class ArrowArrayLoader implements IArrayLoader<ValueVector> {

  private final ListVector vector;
  private final BufferAllocator allocator;
  private final ArrayContainerField schema;
  private final int loadSize;

  /**
   * Init.
   */
  public ArrowArrayLoader(
      final ValueVector vector ,
      final BufferAllocator allocator ,
      final IField schema ,
      final int loadSize ) {
    this.vector = (ListVector)vector;
    this.vector.allocateNew();
    this.vector.setValueCount( loadSize );
    this.allocator = allocator;
    this.schema = (ArrayContainerField)schema;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull( final int index ) throws IOException {
  }

  @Override
  public void finish() throws IOException {}

  @Override
  public ValueVector build() throws IOException {
    return vector;
  }

  @Override
  public void setArrayIndex(
      final int index , final int start , final int length ) throws IOException {
    vector.startNewValue( index );
    vector.endValue( index , length );
  }

  @Override
  public void loadChild(
      final ColumnBinary columnBinary , final int childLoadSize ) throws IOException {
    if ( schema == null ) {
      ILoaderFactory<ValueVector> factory = ArrowLoaderFactoryUtil.createLoaderFactory(
          vector , allocator , columnBinary.columnName , null , columnBinary.columnType );
      factory.create( columnBinary , childLoadSize );
    } else {
      ILoaderFactory<ValueVector> factory =
          ArrowLoaderFactoryUtil.createLoaderFactory( vector , allocator , schema.getField() );
      factory.create( columnBinary , childLoadSize );
    }
  }

}
