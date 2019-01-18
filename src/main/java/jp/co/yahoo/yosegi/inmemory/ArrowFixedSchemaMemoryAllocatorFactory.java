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

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;

public final class ArrowFixedSchemaMemoryAllocatorFactory {

  private ArrowFixedSchemaMemoryAllocatorFactory() {}

  /**
   * Function to convert to IMemoryAllocator for Struct vector.
   */
  public static IMemoryAllocator getFromStructVector(
        final IField schema ,
        final String columnName ,
        final BufferAllocator allocator ,
        final StructVector vector ,
        final int rowCount ) throws IOException {
    switch ( schema.getFieldType() ) {
      case UNION:
        return NullMemoryAllocator.INSTANCE;
      case ARRAY:
        return new ArrowFixedSchemaArrayMemoryAllocator(
            (ArrayContainerField)schema ,
            allocator ,
            vector.addOrGetList( columnName ) ,
            rowCount );
      case MAP:
        StructVector mapVector = vector.addOrGetStruct( columnName );
        return new ArrowFixedSchemaMapMemoryAllocator(
            (MapContainerField)schema , allocator , mapVector , rowCount );
      case STRUCT:
        StructVector structVector = vector.addOrGetStruct( columnName );
        return new ArrowFixedSchemaStructMemoryAllocator(
            (StructContainerField)schema , allocator , structVector , rowCount );

      case BOOLEAN:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.BOOLEAN , columnName ,  allocator , vector , rowCount );
      case BYTE:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.BYTE , columnName ,  allocator , vector , rowCount );
      case SHORT:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.SHORT , columnName ,  allocator , vector , rowCount );
      case INTEGER:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.INTEGER , columnName ,  allocator , vector , rowCount );
      case LONG:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.LONG , columnName ,  allocator , vector , rowCount );
      case FLOAT:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.FLOAT , columnName ,  allocator , vector , rowCount );
      case DOUBLE:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.DOUBLE , columnName ,  allocator , vector , rowCount );
      case STRING:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.STRING , columnName ,  allocator , vector , rowCount );
      case BYTES:
        return ArrowMemoryAllocatorFactory.getFromStructVector(
            ColumnType.BYTES , columnName ,  allocator , vector , rowCount );

      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

  /**
   * Function to convert to IMemoryAllocator for List vector.
   */
  public static IMemoryAllocator getFromListVector(
        final IField schema ,
        final String columnName ,
        final BufferAllocator allocator ,
        final ListVector vector ,
        final int rowCount ) throws IOException {
    switch ( schema.getFieldType() ) {
      case UNION:
        return NullMemoryAllocator.INSTANCE;
      case ARRAY:
        AddOrGetResult<ListVector> listVector =  vector.addOrGetVector(
            new FieldType( true , ArrowType.List.INSTANCE , null , null ) );
        return new ArrowFixedSchemaArrayMemoryAllocator(
            (ArrayContainerField)schema , allocator , listVector.getVector() , rowCount );
      case MAP:
        AddOrGetResult<StructVector> mapVector =  vector.addOrGetVector(
            new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) );
        return new ArrowFixedSchemaMapMemoryAllocator(
            (MapContainerField)schema , allocator , mapVector.getVector() , rowCount );
      case STRUCT:
        AddOrGetResult<StructVector> structVector =  vector.addOrGetVector(
            new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) );
        return new ArrowFixedSchemaStructMemoryAllocator(
            (StructContainerField)schema , allocator , structVector.getVector() , rowCount );

      case BOOLEAN:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.BOOLEAN , columnName ,  allocator , vector , rowCount );
      case BYTE:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.BYTE , columnName ,  allocator , vector , rowCount );
      case SHORT:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.SHORT , columnName ,  allocator , vector , rowCount );
      case INTEGER:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.INTEGER , columnName ,  allocator , vector , rowCount );
      case LONG:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.LONG , columnName ,  allocator , vector , rowCount );
      case FLOAT:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.FLOAT , columnName ,  allocator , vector , rowCount );
      case DOUBLE:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.DOUBLE , columnName ,  allocator , vector , rowCount );
      case STRING:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.STRING , columnName ,  allocator , vector , rowCount );
      case BYTES:
        return ArrowMemoryAllocatorFactory.getFromListVector(
            ColumnType.BYTES , columnName ,  allocator , vector , rowCount );

      default:
        return NullMemoryAllocator.INSTANCE;
    }
  }

}
