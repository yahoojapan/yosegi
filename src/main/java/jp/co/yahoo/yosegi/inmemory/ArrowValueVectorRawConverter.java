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
import jp.co.yahoo.yosegi.message.design.StructContainerField;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;
import java.util.List;

public class ArrowValueVectorRawConverter implements IRawConverter<ValueVector> {

  private final StructVector root;
  private final BufferAllocator allocator;
  private final StructContainerField schema;

  /**
   * Init.
   */
  public ArrowValueVectorRawConverter(
        final BufferAllocator allocator ,
        final StructContainerField schema ) {
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    this.root = new StructVector(
        "root" , allocator , new FieldType( true , Struct.INSTANCE , null , null ) , callBack );
    this.allocator = allocator;
    this.schema = schema;
  }

  @Override
  public ValueVector convert(
      final List<ColumnBinary> raw , final int loadSize ) throws IOException {
    ArrowStructLoader loader = new ArrowStructLoader( root , allocator , schema , loadSize );
    for ( ColumnBinary child : raw ) {
      loader.loadChild( child , loadSize );
    }
    return loader.build();
  }

}
