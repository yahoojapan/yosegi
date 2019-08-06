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
import jp.co.yahoo.yosegi.spread.expression.AllExpressionIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestOptimizedNullArrayCellManager {

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
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 0 , valueArray );
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
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 10 , valueArray );
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
    valueArray[0] = null;
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 0 , valueArray );
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
    valueArray[9] = null;
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 0 , valueArray );
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
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 0 , valueArray );
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
    valueArray[0] = null;
    valueArray[1] = new StringObj( "b" );
    valueArray[2] = new StringObj( "c" );
    valueArray[3] = new StringObj( "d" );
    valueArray[4] = new StringObj( "e" );
    valueArray[5] = new StringObj( "f" );
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 10 , valueArray );
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
    valueArray[9] = null;
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 10 , valueArray );
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
    valueArray[6] = new StringObj( "g" );
    valueArray[7] = new StringObj( "h" );
    valueArray[8] = new StringObj( "i" );
    valueArray[9] = new StringObj( "j" );
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 10 , valueArray );
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
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 0 , valueArray );
    AllExpressionIndex index = new AllExpressionIndex( cellManager.size() );
    PrimitiveObject[] objArray = cellManager.getPrimitiveObjectArray( index , 0 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "a" ,  objArray[0].getString() );
    assertEquals( "b" ,  objArray[1].getString() );
    assertEquals( "c" ,  objArray[2].getString() );
    assertEquals( "d" ,  objArray[3].getString() );
    assertEquals( "e" ,  objArray[4].getString() );

    objArray = cellManager.getPrimitiveObjectArray( index , 5 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "f" ,  objArray[0].getString() );
    assertEquals( "g" ,  objArray[1].getString() );
    assertEquals( "h" ,  objArray[2].getString() );
    assertEquals( "i" ,  objArray[3].getString() );
    assertEquals( "j" ,  objArray[4].getString() );
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
    OptimizedNullArrayCellManager cellManager = new OptimizedNullArrayCellManager(
        ColumnType.STRING , 10 , valueArray );
    AllExpressionIndex index = new AllExpressionIndex( cellManager.size() );
    PrimitiveObject[] objArray = cellManager.getPrimitiveObjectArray( index , 0 , 5 );
    for ( int i = 0 ; i < objArray.length ; i++ ) {
      assertNull( objArray[i] );
    } 

    objArray = cellManager.getPrimitiveObjectArray( index , 5 , 5 );
    for ( int i = 0 ; i < objArray.length ; i++ ) {
      assertNull( objArray[i] );
    } 

    objArray = cellManager.getPrimitiveObjectArray( index , 10 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "a" ,  objArray[0].getString() );
    assertEquals( "b" ,  objArray[1].getString() );
    assertEquals( "c" ,  objArray[2].getString() );
    assertEquals( "d" ,  objArray[3].getString() );
    assertEquals( "e" ,  objArray[4].getString() );

    objArray = cellManager.getPrimitiveObjectArray( index , 15 , 5 );
    assertEquals( 5 , objArray.length );
    assertEquals( "f" ,  objArray[0].getString() );
    assertEquals( "g" ,  objArray[1].getString() );
    assertEquals( "h" ,  objArray[2].getString() );
    assertEquals( "i" ,  objArray[3].getString() );
    assertEquals( "j" ,  objArray[4].getString() );
  }

}
