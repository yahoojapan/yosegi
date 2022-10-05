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

public class TestArrowSequentialByteLoader {

  public ISequentialLoader<ValueVector> createLoader( final int loadSize ) {
    BufferAllocator allocator = new RootAllocator( 1024 * 1024 * 10 );
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    StructVector parent = new StructVector("root", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    parent.allocateNew();

   return new ArrowSequentialByteLoader(
      ArrowLoaderFactoryUtil.createValueVectorFromStructVector( parent , allocator , "target" , ColumnType.BYTE ),
      loadSize );
  }

  @Test
  public void T_setBytes_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    byte[][] data = new byte[][]{null, "10".getBytes(), null, "20".getBytes(), null};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromBytes(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Byte.valueOf(new String(data[1])), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Byte.valueOf(new String(data[3])), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setBytes_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    byte[][] data = new byte[][]{null, "10".getBytes(), null, "128".getBytes(), null};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromBytes(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Byte.valueOf(new String(data[1])), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setString_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    String[] data = new String[]{null, "10", null, "20", null};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromString(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Byte.valueOf(new String(data[1])), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Byte.valueOf(new String(data[3])), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setString_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    String[] data = new String[]{null, "10", null, "128", null};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromString(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Byte.valueOf(new String(data[1])), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setByte_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    byte[] data = new byte[]{(byte) 0, (byte) 10, (byte) 0, (byte) 20, (byte) 0};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromByte(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(data[1], ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(data[3], ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setShort_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    short[] data = new short[]{(short) 0, (short) 10, (short) 0, (short) 20, (short) 0};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromShort(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Short.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Short.valueOf(data[3]).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setShort_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    short[] data = new short[]{(short) 0, (short) 10, (short) 0, (short) 128, (short) 0};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromShort(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Short.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setInteger_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    int[] data = new int[]{0, 10, 0, 20, 0};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromInteger(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Integer.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Integer.valueOf(data[3]).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setInteger_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    int[] data = new int[]{0, 10, 0, 128, 0};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromInteger(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Integer.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setLong_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    long[] data = new long[]{0L, 10L, 0L, 20L, 0L};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromLong(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Long.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Long.valueOf(data[3]).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setLong_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    long[] data = new long[]{0L, 10L, 0L, 128L, 0L};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromLong(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Long.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setFloat_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    float[] data = new float[]{0f, 10f, 0f, 20f, 0f};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromFloat(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Float.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Float.valueOf(data[3]).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setFloat_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    float[] data = new float[]{0f, 10f, 0f, 128f, 0f};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromFloat(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Float.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setDouble_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    double[] data = new double[]{0d, 10d, 0d, 20d, 0d};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromDouble(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Double.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertEquals(Double.valueOf(data[3]).byteValue(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getByte());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setDouble_equalsSetValue_whenOverflow() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    double[] data = new double[]{0d, 10d, 0d, 128d, 0d};
    ISequentialLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowSequentialLoaderTestCase.createVectorFromDouble(loader, data, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Double.valueOf(data[1]).byteValue(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getByte());
    assertNull(column.get(2).getRow());
    assertNull(column.get(3).getRow());
    assertNull(column.get(4).getRow());
  }

}
