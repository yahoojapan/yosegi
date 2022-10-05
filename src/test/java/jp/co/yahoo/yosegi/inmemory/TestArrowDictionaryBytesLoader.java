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

public class TestArrowDictionaryBytesLoader {

  public IDictionaryLoader<ValueVector> createLoader( final int loadSize ) {
    BufferAllocator allocator = new RootAllocator( 1024 * 1024 * 10 );
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    StructVector parent = new StructVector("root", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    parent.allocateNew();

   return new ArrowDictionaryBytesLoader(
      ArrowLoaderFactoryUtil.createValueVectorFromStructVector( parent , allocator , "target" , ColumnType.BYTES ),
      loadSize );
  }

  @Test
  public void T_setBoolean_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    boolean[] dic = new boolean[]{true, false};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromBoolean(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertTrue(( (PrimitiveObject)( column.get(1).getRow() ) ).getBoolean());
    assertNull(column.get(2).getRow());
    assertFalse(( (PrimitiveObject)( column.get(3).getRow() ) ).getBoolean());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setBytes_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    byte[][] dic = new byte[][]{"10".getBytes(), "20".getBytes()};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromBytes(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(new String(dic[ids[1]]), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(new String(dic[ids[3]]), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setString_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    String[] dic = new String[]{"10", "20"};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromString(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(new String(dic[ids[1]]), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(new String(dic[ids[3]]), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setByte_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    byte[] dic = new byte[]{(byte) 10, (byte) 20};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromByte(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Byte.valueOf( dic[ids[1]] ).toString(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(Byte.valueOf( dic[ids[3]] ).toString(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setShort_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    short[] dic = new short[]{(short) 10, (short) 20};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromShort(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Short.valueOf(dic[ids[1]]).toString(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(Short.valueOf(dic[ids[3]]).toString(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setInteger_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    int[] dic = new int[]{10, 20};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromInteger(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Integer.valueOf(dic[ids[1]]).toString(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(Integer.valueOf(dic[ids[3]]).toString(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setLong_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    long[] dic = new long[]{10L, 20L};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromLong(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Long.valueOf(dic[ids[1]]).toString(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(Long.valueOf(dic[ids[3]]).toString(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setFloat_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    float[] dic = new float[]{10f, 20f};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromFloat(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Float.valueOf(dic[ids[1]]).toString(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(Float.valueOf(dic[ids[3]]).toString(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

  @Test
  public void T_setDouble_equalsSetValue() throws IOException {
    boolean[] isNullArray = new boolean[]{true, false, true, false, true};
    double[] dic = new double[]{10d, 20d};
    int[] ids = new int[]{0, 0, 0, 1, 0};
    IDictionaryLoader<ValueVector> loader = createLoader(isNullArray.length);
    IColumn column = ArrowDictionaryLoaderTestCase.createVectorFromDouble(loader, ids, dic, isNullArray);

    assertNull(column.get(0).getRow());
    assertEquals(Double.valueOf(dic[ids[1]]).toString(), ( (PrimitiveObject)( column.get(1).getRow() ) ).getString());
    assertNull(column.get(2).getRow());
    assertEquals(Double.valueOf(dic[ids[3]]).toString(), ( (PrimitiveObject)( column.get(3).getRow() ) ).getString());
    assertNull(column.get(4).getRow());
  }

}
