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

package jp.co.yahoo.yosegi.binary.maker;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.Arrays;

public class TestOptimizedNullArrayDicCellManager {

  private class TestAllocator implements IMemoryAllocator {

    private PrimitiveObject[] objs;

    public TestAllocator( int length ) {
      objs = new PrimitiveObject[length];
    }

    public PrimitiveObject[] getResult() {
      return objs;
    }

    @Override
    public void setNull( final int index ) {
    }

    @Override
    public void setPrimitiveObject(  final int index , final PrimitiveObject value ) throws IOException {
      objs[index] = value;
    }
  }

  @Test
  public void T_get_equalsSetArray_withNotNullArrayAndStartIndexZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 10 );
    assertEquals( "a" , ( (PrimitiveObject)( cellManager.get( 0 , null ).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 1 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 2 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 3 , null ).getRow() ) ).getString() );
    assertEquals( "e" , ( (PrimitiveObject)( cellManager.get( 4 , null ).getRow() ) ).getString() );
    assertEquals( "f" , ( (PrimitiveObject)( cellManager.get( 5 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 6 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 7 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 8 , null ).getRow() ) ).getString() );
    assertEquals( "j" , ( (PrimitiveObject)( cellManager.get( 9 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( -1 , null ) );
    assertNull( cellManager.get( 10 , null ) );
  }

  @Test
  public void T_get_equalsSetArray_withNotNullArrayAndStartIndexNotZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        10 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 20 );
    assertNull( cellManager.get( 0 , null ) );
    assertNull( cellManager.get( 1 , null ) );
    assertNull( cellManager.get( 2 , null ) );
    assertNull( cellManager.get( 3 , null ) );
    assertNull( cellManager.get( 4 , null ) );
    assertNull( cellManager.get( 5 , null ) );
    assertNull( cellManager.get( 6 , null ) );
    assertNull( cellManager.get( 7 , null ) );
    assertNull( cellManager.get( 8 , null ) );
    assertNull( cellManager.get( 9 , null ) );
    assertEquals( "a" , ( (PrimitiveObject)( cellManager.get( 10 , null ).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 11 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 12 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 13 , null ).getRow() ) ).getString() );
    assertEquals( "e" , ( (PrimitiveObject)( cellManager.get( 14 , null ).getRow() ) ).getString() );
    assertEquals( "f" , ( (PrimitiveObject)( cellManager.get( 15 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 16 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 17 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 18 , null ).getRow() ) ).getString() );
    assertEquals( "j" , ( (PrimitiveObject)( cellManager.get( 19 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( -1 , null ) );
    assertNull( cellManager.get( 20 , null ) );
  }

  @Test
  public void T_get_equalsSetArray_withHeadIndexIsNullArrayAndStartIndexZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    isNullArray[0] = true;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 10 );
    assertNull( cellManager.get( 0 , null ) );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 1 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 2 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 3 , null ).getRow() ) ).getString() );
    assertEquals( "e" , ( (PrimitiveObject)( cellManager.get( 4 , null ).getRow() ) ).getString() );
    assertEquals( "f" , ( (PrimitiveObject)( cellManager.get( 5 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 6 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 7 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 8 , null ).getRow() ) ).getString() );
    assertEquals( "j" , ( (PrimitiveObject)( cellManager.get( 9 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( -1 , null ) );
    assertNull( cellManager.get( 10 , null ) );
  }

  @Test
  public void T_get_equalsSetArray_withHeadIndexIsNullArrayAndLastIndexZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    isNullArray[9] = true;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 10 );
    assertEquals( "a" , ( (PrimitiveObject)( cellManager.get( 0 , null ).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 1 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 2 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 3 , null ).getRow() ) ).getString() );
    assertEquals( "e" , ( (PrimitiveObject)( cellManager.get( 4 , null ).getRow() ) ).getString() );
    assertEquals( "f" , ( (PrimitiveObject)( cellManager.get( 5 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 6 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 7 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 8 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( 9 , null ) );
  }

  @Test
  public void T_get_equalsSetArray_withHeadCenterIsNullArrayAndLastIndexZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    isNullArray[4] = true;
    isNullArray[5] = true;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 10 );
    assertEquals( "a" , ( (PrimitiveObject)( cellManager.get( 0 , null ).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 1 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 2 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 3 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 6 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 7 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 8 , null ).getRow() ) ).getString() );
    assertEquals( "j" , ( (PrimitiveObject)( cellManager.get( 9 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( 4 , null ) );
    assertNull( cellManager.get( 5 , null ) );
  }

  @Test
  public void T_get_equalsSetArray_withHeadIndexIsNullArrayAndStartIndexIsNotZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    isNullArray[0] = true;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        10 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 20 );
    assertNull( cellManager.get( 10 , null ) );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 11 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 12 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 13 , null ).getRow() ) ).getString() );
    assertEquals( "e" , ( (PrimitiveObject)( cellManager.get( 14 , null ).getRow() ) ).getString() );
    assertEquals( "f" , ( (PrimitiveObject)( cellManager.get( 15 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 16 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 17 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 18 , null ).getRow() ) ).getString() );
    assertEquals( "j" , ( (PrimitiveObject)( cellManager.get( 19 , null ).getRow() ) ).getString() );
  }

  @Test
  public void T_get_equalsSetArray_withHeadIndexIsNullArrayAndLastIndexIsNotZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    isNullArray[9] = true;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        10 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 20 );
    assertEquals( "a" , ( (PrimitiveObject)( cellManager.get( 10 , null ).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 11 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 12 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 13 , null ).getRow() ) ).getString() );
    assertEquals( "e" , ( (PrimitiveObject)( cellManager.get( 14 , null ).getRow() ) ).getString() );
    assertEquals( "f" , ( (PrimitiveObject)( cellManager.get( 15 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 16 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 17 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 18 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( 19 , null ) );
  }

  @Test
  public void T_get_equalsSetArray_withHeadCenterIsNullArrayAndLastIndexIsNotZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    isNullArray[4] = true;
    isNullArray[5] = true;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        10 ,
        isNullArray,
        indexArray,
        valueArray );
    assertEquals( cellManager.size() , 20 );
    assertEquals( "a" , ( (PrimitiveObject)( cellManager.get( 10 , null ).getRow() ) ).getString() );
    assertEquals( "b" , ( (PrimitiveObject)( cellManager.get( 11 , null ).getRow() ) ).getString() );
    assertEquals( "c" , ( (PrimitiveObject)( cellManager.get( 12 , null ).getRow() ) ).getString() );
    assertEquals( "d" , ( (PrimitiveObject)( cellManager.get( 13 , null ).getRow() ) ).getString() );
    assertEquals( "g" , ( (PrimitiveObject)( cellManager.get( 16 , null ).getRow() ) ).getString() );
    assertEquals( "h" , ( (PrimitiveObject)( cellManager.get( 17 , null ).getRow() ) ).getString() );
    assertEquals( "i" , ( (PrimitiveObject)( cellManager.get( 18 , null ).getRow() ) ).getString() );
    assertEquals( "j" , ( (PrimitiveObject)( cellManager.get( 19 , null ).getRow() ) ).getString() );
    assertNull( cellManager.get( 14 , null ) );
    assertNull( cellManager.get( 15 , null ) );
  }

  @Test
  public void T_getPrimitiveObjectArray_equalsSetArray_withNotNullArrayAndStartIndexZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        valueArray );
    PrimitiveObject[] objArray = cellManager.getPrimitiveObjectArray( 0 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "a" ,  objArray[0].getString() );
    assertEquals( "b" ,  objArray[1].getString() );
    assertEquals( "c" ,  objArray[2].getString() );
    assertEquals( "d" ,  objArray[3].getString() );
    assertEquals( "e" ,  objArray[4].getString() );

    objArray = cellManager.getPrimitiveObjectArray( 5 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "f" ,  objArray[0].getString() );
    assertEquals( "g" ,  objArray[1].getString() );
    assertEquals( "h" ,  objArray[2].getString() );
    assertEquals( "i" ,  objArray[3].getString() );
    assertEquals( "j" ,  objArray[4].getString() );
  }

  @Test
  public void T_setPrimitiveObjectArray_equalsSetArray_withNotNullArrayAndStartIndexZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        valueArray );
    TestAllocator allocator = new TestAllocator(5);
    cellManager.setPrimitiveObjectArray( 0 , 5 , allocator );
    PrimitiveObject[] objArray = allocator.getResult();
    assertEquals( 5 , objArray.length );
    assertEquals( "a" ,  objArray[0].getString() );
    assertEquals( "b" ,  objArray[1].getString() );
    assertEquals( "c" ,  objArray[2].getString() );
    assertEquals( "d" ,  objArray[3].getString() );
    assertEquals( "e" ,  objArray[4].getString() );

    allocator = new TestAllocator(3);
    cellManager.setPrimitiveObjectArray( 5 , 3 , allocator );
    objArray = allocator.getResult();
    assertEquals( 3 , objArray.length );
    assertEquals( "f" ,  objArray[0].getString() );
    assertEquals( "g" ,  objArray[1].getString() );
    assertEquals( "h" ,  objArray[2].getString() );

    allocator = new TestAllocator(3);
    cellManager.setPrimitiveObjectArray( 8 , 3 , allocator );
    objArray = allocator.getResult();
    assertEquals( 3 , objArray.length );
    assertEquals( "i" ,  objArray[0].getString() );
    assertEquals( "j" ,  objArray[1].getString() );
    assertNull( objArray[2] );
  }

  @Test
  public void T_getPrimitiveObjectArray_equalsSetArray_withNotNullArrayAndStartIndexNotZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        10 ,
        isNullArray,
        indexArray,
        valueArray );
    PrimitiveObject[] objArray = cellManager.getPrimitiveObjectArray( 0 , 5 );
    for ( int i = 0 ; i < objArray.length ; i++ ) {
      assertNull( objArray[i] );
    } 

    objArray = cellManager.getPrimitiveObjectArray( 5 , 5 );
    for ( int i = 0 ; i < objArray.length ; i++ ) {
      assertNull( objArray[i] );
    } 

    objArray = cellManager.getPrimitiveObjectArray( 10 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "a" ,  objArray[0].getString() );
    assertEquals( "b" ,  objArray[1].getString() );
    assertEquals( "c" ,  objArray[2].getString() );
    assertEquals( "d" ,  objArray[3].getString() );
    assertEquals( "e" ,  objArray[4].getString() );

    objArray = cellManager.getPrimitiveObjectArray( 15 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "f" ,  objArray[0].getString() );
    assertEquals( "g" ,  objArray[1].getString() );
    assertEquals( "h" ,  objArray[2].getString() );
    assertEquals( "i" ,  objArray[3].getString() );
    assertEquals( "j" ,  objArray[4].getString() );
  }

  @Test
  public void T_setPrimitiveObjectArray_equalsSetArray_withNotNullArrayAndStartIndexNotZero() throws IOException {
    PrimitiveObject[] valueArray = new PrimitiveObject[10];
    valueArray[0] = new StringObj( "a" );
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,1,2,3,4,5,6,7,8,9};
    boolean[] isNullArray = new boolean[10];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        10 ,
        isNullArray,
        indexArray,
        valueArray );
    TestAllocator allocator = new TestAllocator(5);
    cellManager.setPrimitiveObjectArray( 0 , 5 , allocator );
    PrimitiveObject[] objArray = allocator.getResult();
    for ( int i = 0 ; i < objArray.length ; i++ ) {
      assertNull( objArray[i] );
    }

    allocator = new TestAllocator(5);
    cellManager.setPrimitiveObjectArray( 5 , 5 , allocator );
    objArray = allocator.getResult();
    for ( int i = 0 ; i < objArray.length ; i++ ) {
      assertNull( objArray[i] );
    }

    allocator = new TestAllocator(5);
    cellManager.setPrimitiveObjectArray( 10 , 5 , allocator );
    objArray = allocator.getResult();
    assertEquals( "a" ,  objArray[0].getString() );
    assertEquals( "b" ,  objArray[1].getString() );
    assertEquals( "c" ,  objArray[2].getString() );
    assertEquals( "d" ,  objArray[3].getString() );
    assertEquals( "e" ,  objArray[4].getString() );

    allocator = new TestAllocator(3);
    cellManager.setPrimitiveObjectArray( 15 , 3 , allocator );
    objArray = allocator.getResult();
    assertEquals( "f" ,  objArray[0].getString() );
    assertEquals( "g" ,  objArray[1].getString() );
    assertEquals( "h" ,  objArray[2].getString() );

    allocator = new TestAllocator(3);
    cellManager.setPrimitiveObjectArray( 18 , 3 , allocator );
    objArray = allocator.getResult();
    assertEquals( "i" ,  objArray[0].getString() );
    assertEquals( "j" ,  objArray[1].getString() );
    assertNull( objArray[2] );
  }

  @Test
  public void T_getDictionary_equalsSetArray_withNotNullAndStartIndexZero() throws IOException {
    PrimitiveObject[] dicArray = new PrimitiveObject[10];
    dicArray[0] = new StringObj( "a" );
    dicArray[1] = new StringObj( "b" );
    dicArray[2] = new StringObj( "c" );
    dicArray[3] = new StringObj( "d" );
    dicArray[4] = new StringObj( "e" );
    dicArray[5] = new StringObj( "f" );
    dicArray[6] = new StringObj( "g" );
    dicArray[7] = new StringObj( "h" );
    dicArray[8] = new StringObj( "i" );
    dicArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9};
    boolean[] isNullArray = new boolean[20];
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        0 ,
        isNullArray,
        indexArray,
        dicArray );
    assertTrue( cellManager.isDictionary() );
    assertEquals( cellManager.getDictionarySize() , 10 );
    assertEquals( cellManager.size() , 20 );
    boolean[] newIsNullArray = cellManager.getDictionaryIsNullArray();
    int[] newIndexArray = cellManager.getDictionaryIndexArray();
    PrimitiveObject[] newDicArray = cellManager.getDictionaryArray();
    for ( int i = 0 ; i < cellManager.size() ; i++ ) {
      assertFalse( newIsNullArray[i] );
      assertEquals( dicArray[indexArray[i]].getString() , newDicArray[newIndexArray[i]].getString() );
    }
  }

  @Test
  public void T_getDictionary_equalsSetArray_withNotNullAndStartIndexTen() throws IOException {
    PrimitiveObject[] dicArray = new PrimitiveObject[10];
    dicArray[0] = new StringObj( "a" );
    dicArray[1] = new StringObj( "b" );
    dicArray[2] = new StringObj( "c" );
    dicArray[3] = new StringObj( "d" );
    dicArray[4] = new StringObj( "e" );
    dicArray[5] = new StringObj( "f" );
    dicArray[6] = new StringObj( "g" );
    dicArray[7] = new StringObj( "h" );
    dicArray[8] = new StringObj( "i" );
    dicArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9};
    boolean[] isNullArray = new boolean[20];
    int startIndex = 10;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        startIndex ,
        isNullArray,
        indexArray,
        dicArray );
    assertTrue( cellManager.isDictionary() );
    assertEquals( cellManager.getDictionarySize() , 10 );
    assertEquals( cellManager.size() , 30 );
    boolean[] newIsNullArray = cellManager.getDictionaryIsNullArray();
    int[] newIndexArray = cellManager.getDictionaryIndexArray();
    PrimitiveObject[] newDicArray = cellManager.getDictionaryArray();
    for ( int i = 0 ; i < cellManager.size() ; i++ ) {
      if ( i < 10 ) {
        assertTrue( newIsNullArray[i] );
      } else {
        assertFalse( newIsNullArray[i] );
        assertEquals( dicArray[indexArray[i-startIndex]].getString() , newDicArray[newIndexArray[i]].getString() );
      }
    }
  }

  @Test
  public void T_getDictionary_equalsSetArray_withNullAndStartIndexZero() throws IOException {
    PrimitiveObject[] dicArray = new PrimitiveObject[10];
    dicArray[0] = new StringObj( "a" );
    dicArray[1] = new StringObj( "b" );
    dicArray[2] = new StringObj( "c" );
    dicArray[3] = new StringObj( "d" );
    dicArray[4] = new StringObj( "e" );
    dicArray[5] = new StringObj( "f" );
    dicArray[6] = new StringObj( "g" );
    dicArray[7] = new StringObj( "h" );
    dicArray[8] = new StringObj( "i" );
    dicArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9};
    boolean[] isNullArray = new boolean[20];
    for ( int i = 0 ; i < isNullArray.length ; i++ ) {
      if ( ( i % 2 ) == 0 ) {
        isNullArray[i] = true;
      }
    }
    int startIndex = 0;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        startIndex ,
        isNullArray,
        indexArray,
        dicArray );
    assertTrue( cellManager.isDictionary() );
    assertEquals( cellManager.getDictionarySize() , 10 );
    assertEquals( cellManager.size() , 20 );
    boolean[] newIsNullArray = cellManager.getDictionaryIsNullArray();
    int[] newIndexArray = cellManager.getDictionaryIndexArray();
    PrimitiveObject[] newDicArray = cellManager.getDictionaryArray();
    for ( int i = 0 ; i < cellManager.size() ; i++ ) {
      if ( ( i % 2 ) == 0 ) {
        assertTrue( newIsNullArray[i] );
      } else {
        assertFalse( newIsNullArray[i] );
        assertEquals( dicArray[indexArray[i-startIndex]].getString() , newDicArray[newIndexArray[i]].getString() );
      }
    }
  }

  @Test
  public void T_getDictionary_equalsSetArray_withNullAndStartIndexTen() throws IOException {
    PrimitiveObject[] dicArray = new PrimitiveObject[10];
    dicArray[0] = new StringObj( "a" );
    dicArray[1] = new StringObj( "b" );
    dicArray[2] = new StringObj( "c" );
    dicArray[3] = new StringObj( "d" );
    dicArray[4] = new StringObj( "e" );
    dicArray[5] = new StringObj( "f" );
    dicArray[6] = new StringObj( "g" );
    dicArray[7] = new StringObj( "h" );
    dicArray[8] = new StringObj( "i" );
    dicArray[9] = new StringObj( "j" );
    int[] indexArray = new int[]{0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9};
    boolean[] isNullArray = new boolean[20];
    for ( int i = 0 ; i < isNullArray.length ; i++ ) {
      if ( ( i % 2 ) == 0 ) {
        isNullArray[i] = true;
      }
    }
    int startIndex = 10;
    OptimizedNullArrayDicCellManager cellManager = new OptimizedNullArrayDicCellManager(
        ColumnType.STRING ,
        startIndex ,
        isNullArray,
        indexArray,
        dicArray );
    assertTrue( cellManager.isDictionary() );
    assertEquals( cellManager.getDictionarySize() , 10 );
    assertEquals( cellManager.size() , 30 );
    boolean[] newIsNullArray = cellManager.getDictionaryIsNullArray();
    int[] newIndexArray = cellManager.getDictionaryIndexArray();
    PrimitiveObject[] newDicArray = cellManager.getDictionaryArray();
    for ( int i = 0 ; i < cellManager.size() ; i++ ) {
      if ( i < 10 ) {
        assertTrue( newIsNullArray[i] );
      } else {
        if ( ( i % 2 ) == 0 ) {
          assertTrue( newIsNullArray[i] );
        } else {
          assertFalse( newIsNullArray[i] );
          assertEquals( dicArray[indexArray[i-startIndex]].getString() , newDicArray[newIndexArray[i]].getString() );
        }
      }
    }
  }

}
