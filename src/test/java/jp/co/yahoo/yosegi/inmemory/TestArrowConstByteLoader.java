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

public class TestArrowConstByteLoader {

  public IConstLoader<ValueVector> createLoader( final int loadSize ) {
    BufferAllocator allocator = new RootAllocator( 1024 * 1024 * 10 );
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    StructVector parent = new StructVector("root", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    parent.allocateNew();

   return new ArrowConstByteLoader(
      ArrowLoaderFactoryUtil.createValueVectorFromStructVector( parent , allocator , "target" , ColumnType.BYTE ),
      loadSize );
  }

  @Test
  public void T_setBytes_equalsSetValue() throws IOException {
    byte[] data = "10".getBytes();
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromBytes(loader, data);

    assertEquals(Byte.valueOf(new String(data)), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(new String(data)), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(new String(data)), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(new String(data)), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(new String(data)), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setBytes_equalsSetValue_whenOverflow() throws IOException {
    byte[] data = "128".getBytes();
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromBytes(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setString_equalsSetValue() throws IOException {
    String data = "10";
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromString(loader, data);

    assertEquals(Byte.valueOf(data), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setString_equalsSetValue_whenOverflow() throws IOException {
    String data = "128";
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromString(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setByte_equalsSetValue() throws IOException {
    byte data = (byte) 10;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromByte(loader, data);

    assertEquals(Byte.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Byte.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setShort_equalsSetValue() throws IOException {
    short data = (short) 10;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromShort(loader, data);

    assertEquals(Short.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Short.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Short.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Short.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Short.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setShort_equalsSetValue_whenOverflow() throws IOException {
    short data = (short) 128;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromShort(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setInteger_equalsSetValue() throws IOException {
    int data = 10;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromInteger(loader, data);

    assertEquals(Integer.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Integer.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Integer.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Integer.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Integer.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setInteger_equalsSetValue_whenOverflow() throws IOException {
    int data = 128;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromInteger(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setLong_equalsSetValue() throws IOException {
    long data = 10L;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromLong(loader, data);

    assertEquals(Long.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Long.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Long.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Long.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Long.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setLong_equalsSetValue_whenOverflow() throws IOException {
    long data = 128L;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromLong(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setFloat_equalsSetValue() throws IOException {
    float data = 10f;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromFloat(loader, data);

    assertEquals(Float.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Float.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Float.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Float.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Float.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setFloat_equalsSetValue_whenOverflow() throws IOException {
    float data = 128f;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromFloat(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setDouble_equalsSetValue() throws IOException {
    double data = 10d;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromDouble(loader, data);

    assertEquals(Double.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(0).getRow() ) ).getByte());
    assertEquals(Double.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertEquals(Double.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(2).getRow() ) ).getByte());
    assertEquals(Double.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertEquals(Double.valueOf(data).byteValue(), ( (PrimitiveObject)( column.get(4).getRow() ) ).getByte());
  }

  @Test
  public void T_setDouble_equalsSetValue_whenOverflow() throws IOException {
    double data = 128d;
    IConstLoader<ValueVector> loader = createLoader(5);
    IColumn column = ArrowConstLoaderTestCase.createVectorFromDouble(loader, data);

    assertNull(column.get(0).getRow());
    assertNull(column.get(1).getRow());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

}
