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

import jp.co.yahoo.yosegi.spread.column.ColumnType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.FieldType;

public final class ArrowMemoryAllocatorFactory {

  private ArrowMemoryAllocatorFactory() {}

  /**
   * Set the vector of Struct and initialize it.
   */
  public static IMemoryAllocator getFromStructVector(
      final ColumnType columnType ,
      final String columnName ,
      final BufferAllocator allocator ,
      final StructVector vector ,
      final int rowCount ) {
    switch ( columnType ) {
      case UNION:
        UnionVector unionVector = vector.addOrGetUnion( columnName );
        return new ArrowUnionMemoryAllocator( allocator , unionVector , rowCount );
      case ARRAY:
        return new ArrowArrayMemoryAllocator(
            allocator , vector.addOrGetList( columnName ) , rowCount );
      case SPREAD:
        StructVector mapVector = vector.addOrGetStruct( columnName );
        return new ArrowMapMemoryAllocator( allocator , mapVector , rowCount );

      case BOOLEAN:
        BitVector bitVector =  vector.addOrGet(
            columnName ,
            new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) ,
            BitVector.class );
        return new ArrowBooleanMemoryAllocator( bitVector , rowCount );
      case BYTE:
        TinyIntVector byteVector =  vector.addOrGet(
            columnName ,
            new FieldType( true , new ArrowType.Int( 8 , true ) , null , null ) ,
            TinyIntVector.class );
        return new ArrowByteMemoryAllocator( byteVector , rowCount );
      case SHORT:
        SmallIntVector shortVector = vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.Int( 16 , true ) ,
              null ,
              null ) ,
            SmallIntVector.class );
        return new ArrowShortMemoryAllocator( shortVector , rowCount );
      case INTEGER:
        IntVector integerVector =  vector.addOrGet(
            columnName ,
            new FieldType( true , new ArrowType.Int( 32 , true ) , null , null ) ,
            IntVector.class );
        return new ArrowIntegerMemoryAllocator( integerVector , rowCount );
      case LONG:
        BigIntVector longVector = vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.Int( 64 , true ) ,
              null ,
              null ) ,
              BigIntVector.class );
        return new ArrowLongMemoryAllocator( longVector , rowCount );
      case FLOAT:
        Float4Vector floatVector = vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.SINGLE ) ,
              null ,
              null ) ,
            Float4Vector.class );
        return new ArrowFloatMemoryAllocator( floatVector , rowCount );
      case DOUBLE:
        Float8Vector doubleVector = vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) ,
              null ,
              null ) ,
            Float8Vector.class );
        return new ArrowDoubleMemoryAllocator( doubleVector , rowCount );
      case STRING:
        VarCharVector charVector = vector.addOrGet(
            columnName ,
            new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) ,
            VarCharVector.class );
        return new ArrowStringMemoryAllocator( charVector , rowCount );
      case BYTES:
        VarBinaryVector binaryVector = vector.addOrGet(
            columnName ,
            new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) ,
            VarBinaryVector.class );
        return new ArrowBytesMemoryAllocator( binaryVector , rowCount );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

  /**
   * Set the vector of List and initialize it.
   */
  public static IMemoryAllocator getFromListVector(
        final ColumnType columnType ,
        final String columnName ,
        final BufferAllocator allocator ,
        final ListVector vector ,
        final int rowCount ) {
    switch ( columnType ) {
      case UNION:
        AddOrGetResult<UnionVector> unionVector =  vector.addOrGetVector(
            new FieldType( true , MinorType.UNION.getType() , null , null ) );
        return new ArrowUnionMemoryAllocator( allocator , unionVector.getVector() , rowCount );
      case ARRAY:
        AddOrGetResult<ListVector> listVector =  vector.addOrGetVector(
            new FieldType( true , ArrowType.List.INSTANCE , null , null ) );
        return new ArrowArrayMemoryAllocator( allocator , listVector.getVector() , rowCount );
      case SPREAD:
        AddOrGetResult<StructVector> mapVector = vector.addOrGetVector(
            new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) );
        return new ArrowMapMemoryAllocator( allocator , mapVector.getVector() , rowCount );

      case BOOLEAN:
        AddOrGetResult<BitVector> bitVector = vector.addOrGetVector(
            new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) );
        return new ArrowBooleanMemoryAllocator( bitVector.getVector() , rowCount );
      case BYTE:
        AddOrGetResult<TinyIntVector> byteVector = vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 8 , true ) , null , null ) );
        return new ArrowByteMemoryAllocator( byteVector.getVector() , rowCount );
      case SHORT:
        AddOrGetResult<SmallIntVector> shortVector = vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 16 , true ) , null , null ) );
        return new ArrowShortMemoryAllocator( shortVector.getVector() , rowCount );
      case INTEGER:
        AddOrGetResult<IntVector> integerVector =  vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 32 , true ) , null , null ) );
        return new ArrowIntegerMemoryAllocator( integerVector.getVector() , rowCount );
      case LONG:
        AddOrGetResult<BigIntVector> longVector =  vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 64 , true ) , null , null ) );
        return new ArrowLongMemoryAllocator( longVector.getVector() , rowCount );
      case FLOAT:
        AddOrGetResult<Float4Vector> floatVector = vector.addOrGetVector(
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.HALF ) ,
              null ,
              null ) );
        return new ArrowFloatMemoryAllocator( floatVector.getVector() , rowCount );
      case DOUBLE:
        AddOrGetResult<Float8Vector> doubleVector = vector.addOrGetVector(
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) ,
              null ,
              null ) );
        return new ArrowDoubleMemoryAllocator( doubleVector.getVector() , rowCount );
      case STRING:
        AddOrGetResult<VarCharVector> charVector = vector.addOrGetVector(
            new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) );
        return new ArrowStringMemoryAllocator( charVector.getVector() , rowCount );
      case BYTES:
        AddOrGetResult<VarBinaryVector> binaryVector = vector.addOrGetVector(
            new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) );
        return new ArrowBytesMemoryAllocator( binaryVector.getVector() , rowCount );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

  /**
   * Set the vector of Union and initialize it.
   */
  public static IMemoryAllocator getFromUnionVector(
      final ColumnType columnType ,
      final String columnName ,
      final BufferAllocator allocator ,
      final UnionVector vector ,
      final int rowCount ) {
    switch ( columnType ) {
      case UNION:
        return NullMemoryAllocator.INSTANCE;
      case ARRAY:
        return new ArrowArrayMemoryAllocator( allocator , vector.getList() , rowCount );
      case SPREAD:
        return new ArrowMapMemoryAllocator( allocator , vector.getStruct() , rowCount );

      case BOOLEAN:
        return new ArrowBooleanMemoryAllocator( vector.getBitVector() , rowCount );
      case BYTE:
        return new ArrowByteMemoryAllocator( vector.getTinyIntVector() , rowCount );
      case SHORT:
        return new ArrowShortMemoryAllocator( vector.getSmallIntVector() , rowCount );
      case INTEGER:
        return new ArrowIntegerMemoryAllocator( vector.getIntVector() , rowCount );
      case LONG:
        return new ArrowLongMemoryAllocator( vector.getBigIntVector() , rowCount );
      case FLOAT:
        return new ArrowFloatMemoryAllocator( vector.getFloat4Vector() , rowCount );
      case DOUBLE:
        return new ArrowDoubleMemoryAllocator( vector.getFloat8Vector() , rowCount );
      case STRING:
        return new ArrowStringMemoryAllocator( vector.getVarCharVector() , rowCount );
      case BYTES:
        return new ArrowBytesMemoryAllocator( vector.getVarBinaryVector() , rowCount );

      case NULL:
      case EMPTY_ARRAY:
      case EMPTY_SPREAD:
      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

}
