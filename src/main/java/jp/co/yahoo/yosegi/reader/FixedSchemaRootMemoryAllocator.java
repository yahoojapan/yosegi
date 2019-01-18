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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.inmemory.ArrowFixedSchemaStructMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.design.StructContainerField;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.StructVector;

import java.io.IOException;

public class FixedSchemaRootMemoryAllocator implements IRootMemoryAllocator {

  private final StructContainerField schema;

  public FixedSchemaRootMemoryAllocator( final StructContainerField schema ) {
    this.schema = schema;
  }

  @Override
  public IMemoryAllocator create(
      final BufferAllocator allocator ,
      final StructVector rootVector ,
      final int rowCount ) throws IOException {
    return new ArrowFixedSchemaStructMemoryAllocator( schema , allocator , rootVector , rowCount );
  }

}
