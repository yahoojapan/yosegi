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

package jp.co.yahoo.yosegi.spread.column;

import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.expression.AllExpressionIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestUnionColumn {

  public class TestAllocator implements IMemoryAllocator {
    public final int[] values;
    public final boolean[] isNull;

    public TestAllocator( final int num ) {
      values = new int[num];
      isNull = new boolean[num];
    }

    @Override
    public void setNull( final int index ) {
      isNull[index] = true;
    }

    @Override
    public void setInteger( final int index , final int value ) throws IOException {
      values[index] = value;
    }

    @Override
    public void setPrimitiveObject(
        final int index , final PrimitiveObject value ) throws IOException {
      setInteger( index , value.getInt() );
    }
  }

  @Test
  public void T_setPrimitiveObject_equalsSetValue() throws IOException {
    PrimitiveColumn u1 = new PrimitiveColumn( ColumnType.STRING , "c1" );
    u1.add( ColumnType.STRING , new StringObj( "100" ) , 1 );

    UnionColumn column = new UnionColumn( u1 );
    column.add( ColumnType.INTEGER , new IntegerObj( 101 ) , 2 );
    column.add( ColumnType.INTEGER , new IntegerObj( 102 ) , 3 );

    AllExpressionIndex index = new AllExpressionIndex( 4 );
    TestAllocator allocator = new TestAllocator( 4 );
    column.setPrimitiveObjectArray( index , 0 , 4 , allocator );    

    assertTrue( allocator.isNull[0] );
    assertEquals( 100 , allocator.values[1] );
    assertEquals( 101 , allocator.values[2] );
    assertEquals( 102 , allocator.values[3] );
  }

}
