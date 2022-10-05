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
import jp.co.yahoo.yosegi.spread.column.ArrowColumnFactory;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.SchemaChangeCallBack;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestArrowBytesVector {

  public IColumn createValueVector(ColumnBinary columnBinary, int loadSize) throws IOException {
    BufferAllocator allocator = new RootAllocator( 1024 * 1024 * 10 );
    SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    StructVector parent = new StructVector("root", allocator, new FieldType(false, Struct.INSTANCE, null, null), callBack);
    parent.allocateNew();

    ValueVector vector = ArrowLoaderFactoryUtil.createLoaderFactory(parent, allocator, new BytesField("vector")).create(columnBinary, loadSize);
    IColumn column = ArrowColumnFactory.convert("vector" , vector);
    assertEquals(vector.getValueCount(), loadSize);
    assertEquals(vector.getValueCount(), column.size());
    assertEquals(loadSize, column.size());
    return column;
  }

  public static void createTestCase(List<Arguments> args, String[] data, boolean[] isNullArray) throws IOException {
    String[] testClassArray = ColumnBinaryTestCase.bytesClassNames();
    for (int i = 0; i < testClassArray.length; i++) {
      args.add(arguments(testClassArray[i], data, isNullArray));
    }
  }

  public static Stream<Arguments> data() throws IOException {
    List<Arguments> args = new ArrayList<Arguments>();
    createTestCase(args, new String[]{"a", "ab", "abc", "abcd", "bc", "bc", "bc", "bc", "ab", ""}, new boolean[]{false, false, false, false, false, false, false, false, false, false});
    createTestCase(args, new String[]{null, "ab", null, "abcd", null, "bc", null, "bc", null, ""}, new boolean[]{true, false, true, false, true, false, true, false, true, false});
    createTestCase(args, new String[]{null, null, null, null, null, "b", "bc", "bc", "bcd", "bcd"}, new boolean[]{true, true, true, true, true, false, false, false, false, false});
    createTestCase(args, new String[]{"b", "bc", "bc", "bcd", "bcd", null, null, null, null, null}, new boolean[]{false, false, false, false, false, true, true, true, true, true});
    createTestCase(args, new String[]{"ab", null, null, null, null, null, null, null, null, "bc"}, new boolean[]{false, true, true, true, true, true, true, true, true, false});

    createTestCase(args, new String[]{"ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab", "ab"}, new boolean[]{false, false, false, false, false, false, false, false, false, false});
    createTestCase(args, new String[]{null, "ab", null, "ab", null, "ab", null, "ab", null, "ab"}, new boolean[]{true, false, true, false, true, false, true, false, true, false});
    createTestCase(args, new String[]{null, null, null, null, null, "ab", "ab", "ab", "ab", "ab"}, new boolean[]{true, true, true, true, true, false, false, false, false, false});
    createTestCase(args, new String[]{"ab", "ab", "ab", "ab", "ab", null, null, null, null, null}, new boolean[]{false, false, false, false, false, true, true, true, true, true});
    createTestCase(args, new String[]{"ab", null, null, null, null, null, null, null, null, "ab"}, new boolean[]{false, true, true, true, true, true, true, true, true, false});
    createTestCase(args, new String[]{null, null, null, null, null, null, null, null, null, "ab"}, new boolean[]{true, true, true, true, true, true, true, true, true, false});
    createTestCase(args, new String[]{"ab", null, null, null, null, null, null, null, null, null}, new boolean[]{false, true, true, true, true, true, true, true, true, true});
    return args.stream();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    IColumn column = createValueVector(columnBinary, isNullArray.length);

    for (int i = 0; i < isNullArray.length; i++) {
      if (isNullArray[i]) {
        assertNull(column.get(i).getRow());
      } else {
        assertNotNull(column.get(i).getRow());
        assertEquals(data[i], ( (PrimitiveObject)( column.get(i).getRow() ) ).getString());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withAllValueIndex(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    columnBinary.setRepetitions(new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 10);
    IColumn column = createValueVector(columnBinary, 10);

    for (int i = 0; i < isNullArray.length; i++) {
      if (isNullArray[i]) {
        assertNull(column.get(i).getRow());
      } else {
        assertNotNull(column.get(i).getRow());
        assertEquals(data[i], ( (PrimitiveObject)( column.get(i).getRow() ) ).getString());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLargeLoadIndex(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    columnBinary.setRepetitions(new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 15);
    IColumn column = createValueVector(columnBinary, 15);

    for (int i = 0; i < isNullArray.length; i++) {
      if (isNullArray[i]) {
        assertNull(column.get(i).getRow());
      } else {
        assertNotNull(column.get(i).getRow());
        assertEquals(data[i], ( (PrimitiveObject)( column.get(i).getRow() ) ).getString());
      }
    }
    for (int i = isNullArray.length; i < 15; i++) {
      assertNull(column.get(i).getRow());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLoadIndexIsHead5(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    columnBinary.setRepetitions(new int[]{1, 1, 1, 1, 1}, 5);
    IColumn column = createValueVector(columnBinary, 5);

    for (int i = 0; i < 5; i++) {
      if (isNullArray[i]) {
        assertNull(column.get(i).getRow());
      } else {
        assertNotNull(column.get(i).getRow());
        assertEquals(data[i], ( (PrimitiveObject)( column.get(i).getRow() ) ).getString());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLoadIndexIsTail5(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    columnBinary.setRepetitions(new int[]{0, 0, 0, 0, 0, 1, 1, 1, 1, 1}, 5);
    IColumn column = createValueVector(columnBinary, 5);

    for (int i = 5; i < 10; i++) {
      if (isNullArray[i]) {
        assertNull(column.get(i - 5).getRow());
      } else {
        assertNotNull(column.get(i - 5).getRow());
        assertEquals(data[i], ( (PrimitiveObject)( column.get(i - 5).getRow() ) ).getString());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withAllValueIndexAndExpand(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    int[] loadIndex = new int[]{2, 1, 2, 1, 2, 1, 2, 1, 2, 1};
    columnBinary.setRepetitions(loadIndex, 15);
    IColumn column = createValueVector(columnBinary, 15);

    int index = 0;
    for (int i = 0; i < loadIndex.length; i++) {
      for (int n = index; n < index + loadIndex[i]; n++) {
        if (isNullArray[i]) {
          assertNull(column.get(n).getRow());
        } else {
          assertNotNull(column.get(n).getRow());
          assertEquals(data[i], ( (PrimitiveObject)( column.get(n).getRow() ) ).getString());
        }
      }
      index += loadIndex[i];
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void T_load_equalsSetValue_withLargeLoadIndexAndExpand(String targetClassName, String[] data, boolean[] isNullArray) throws IOException {
    ColumnBinary columnBinary = ColumnBinaryTestCase.createBytesColumnBinaryFromString(targetClassName, data, isNullArray);
    int[] loadIndex = new int[]{2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
    columnBinary.setRepetitions(loadIndex, 20);
    IColumn column = createValueVector(columnBinary, 20);

    int index = 0;
    for (int i = 0; i < loadIndex.length; i++) {
      for (int n = index; n < index + loadIndex[i]; n++) {
        if (i < isNullArray.length) {
          if (isNullArray[i]) {
            assertNull(column.get(n).getRow());
          } else {
            assertNotNull(column.get(n).getRow());
            assertEquals(data[i], ( (PrimitiveObject)( column.get(n).getRow() ) ).getString());
          }
        } else {
          assertNull(column.get(n).getRow());
        }
      }
      index += loadIndex[i];
    }
  }
}
