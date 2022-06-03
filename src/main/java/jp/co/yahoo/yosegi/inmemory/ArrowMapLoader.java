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
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;

public class ArrowMapLoader implements ISpreadLoader<ValueVector> {

  private final StructVector vector;
  private final BufferAllocator allocator;
  private final MapContainerField schema;
  private final int loadSize;

  /**
   * Init.
   */
  public ArrowMapLoader(
      final ValueVector vector ,
      final BufferAllocator allocator ,
      final IField schema ,
      final int loadSize ) {
    this.vector = (StructVector)vector;
    this.allocator = allocator;
    this.schema = (MapContainerField)schema;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull( final int index ) throws IOException {}

  @Override
  public void finish() throws IOException {}

  @Override
  public ValueVector build() throws IOException {
    return vector;
  }

  @Override
  public void loadChild(
      final ColumnBinary columnBinary , final int loadSize ) throws IOException {
    ILoaderFactory<ValueVector> factory = ArrowLoaderFactoryUtil.createLoaderFactory(
        vector , allocator , schema.get( columnBinary.columnName ) );
    factory.create( columnBinary , loadSize );
  }

}