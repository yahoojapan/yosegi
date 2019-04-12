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

import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;

abstract class ArrowLoader implements IArrowLoader {
  protected final StructVector rootVector;
  protected final YosegiReader reader;
  protected final BufferAllocator allocator;
  protected final IRootMemoryAllocator rootMemoryAllocator;
  protected IExpressionNode node;

  /**
   * FileReader and Arrow memory allocators are set and initialized.
   */
  protected ArrowLoader(
      final IRootMemoryAllocator rootMemoryAllocator,
      final YosegiReader reader,
      final BufferAllocator allocator) {
    this.reader = reader;
    this.allocator = allocator;
    this.rootMemoryAllocator = rootMemoryAllocator;
    FieldType type = new FieldType(true, Struct.INSTANCE, null, null);
    rootVector = new StructVector("root", allocator, type, new SchemaChangeCallBack());
  }

  @Override
  public void setNode(final IExpressionNode node) {
    this.node = node;
  }

  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  @Override
  public void close() throws IOException {
    rootVector.clear();
    reader.close();
  }
}
