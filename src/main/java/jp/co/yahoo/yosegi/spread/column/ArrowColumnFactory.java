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

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.HashMap;
import java.util.Map;

public final class ArrowColumnFactory {

  private static final Map<Class, ColumnFactory> dispatch = new HashMap<>();

  static {
    dispatch.put( BitVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowBooleanConnector( name , (BitVector)vec ) ) );
    dispatch.put( TinyIntVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowByteConnector( name , (TinyIntVector)vec ) ) );
    dispatch.put( SmallIntVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowShortConnector( name , (SmallIntVector)vec ) ) );
    dispatch.put( IntVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowIntegerConnector( name , (IntVector)vec ) ) );
    dispatch.put( BigIntVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowLongConnector( name , (BigIntVector)vec ) ) );
    dispatch.put( Float4Vector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowFloatConnector( name , (Float4Vector)vec ) ) );
    dispatch.put( Float8Vector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowDoubleConnector( name , (Float8Vector)vec ) ) );
    dispatch.put( VarCharVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowStringConnector( name , (VarCharVector)vec ) ) );
    dispatch.put( VarBinaryVector.class , ( name , vec ) ->
        new ArrowPrimitiveColumn( new ArrowBytesConnector( name , (VarBinaryVector)vec ) ) );
    dispatch.put( StructVector.class , ( name , vec ) ->
        new ArrowStructColumn( name , (StructVector)vec ) );
    dispatch.put( ListVector.class , ( name , vec ) ->
        new ArrowArrayColumn( name , (ListVector)vec ) );
  }

  /**
   * Convert Arrow's vector to IColumn.
   */
  public static IColumn convert( final String name , final ValueVector vector ) {
    ColumnFactory factory = dispatch.get( vector.getClass() );
    if ( factory == null ) {
      throw new UnsupportedOperationException(
          "Unsupported vector : " + vector.getClass().getName() );
    }
    return factory.get( name , vector );
  }

  @FunctionalInterface
  private static interface ColumnFactory {
    IColumn get( final String columnName , final ValueVector vector )
        throws UnsupportedOperationException;
  }

}
