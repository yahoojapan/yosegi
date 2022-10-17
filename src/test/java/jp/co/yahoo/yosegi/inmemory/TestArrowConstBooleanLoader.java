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

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

public class TestArrowConstBooleanLoader {

  public IConstLoader<ValueVector> createLoader( final int loadSize ) {
    BufferAllocator allocator = new RootAllocator( 1024 * 1024 * 10 );
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    StructVector parent = new StructVector("root", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    parent.allocateNew();

   return new ArrowConstBooleanLoader(
      ArrowLoaderFactoryUtil.createValueVectorFromStructVector( parent , allocator , "target" , ColumnType.BOOLEAN ),
      loadSize );
  }

  @Test
  public void T_setBoolean_equalsSetValue() throws IOException {
    boolean data = true;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromBoolean(loader, data);

    assertTrue(( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean());
  }

  @Test
  public void T_setBytes_equalsSetValue() throws IOException {
    byte[] data = "true".getBytes();
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromBytes(loader, data);

    assertTrue(( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean());
  }

  @Test
  public void T_setString_equalsSetValue() throws IOException {
    String data = "true";
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromString(loader, data);

    assertTrue(( (PrimitiveObject)( column.get(0).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(2).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean());
    assertTrue(( (PrimitiveObject)( column.get(4).getRow() ) ).getBoolean());
  }

}
