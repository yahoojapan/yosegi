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
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.spread.column.ColumnType;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;

public final class ArrowLoaderFactoryUtil {

  private static class ArrowUnionLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;
    private final IField schema;

    private ArrowUnionLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ,
        final IField schema ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
      this.schema = schema;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case UNION :
          return new ArrowUnionLoader(
              createValueVector( vector , allocator , columnName , ColumnType.UNION ) ,
              allocator ,
              schema ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.UNION ) ,
              loadSize );
      }
    }

  }

  private static class ArrowArrayLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;
    private final IField schema;

    private ArrowArrayLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ,
        final IField schema ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
      this.schema = schema;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case ARRAY :
          return new ArrowArrayLoader(
              createValueVector( vector , allocator , columnName , ColumnType.ARRAY ) ,
              allocator ,
              schema ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.ARRAY ) ,
              loadSize );
      }
    }

  }

  private static class ArrowMapLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;
    private final IField schema;

    private ArrowMapLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ,
        final IField schema ) {
      if ( schema == null ) {
        throw new UnsupportedOperationException(
            "The Map type must have a schema defined." );
      }
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
      this.schema = schema;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SPREAD :
          return new ArrowMapLoader(
              createValueVector( vector , allocator , columnName , ColumnType.MAP ) ,
              allocator ,
              schema ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.MAP ) ,
              loadSize );
      }
    }

  }

  private static class ArrowSpreadLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;
    private final IField schema;

    private ArrowSpreadLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ,
        final IField schema ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
      this.schema = schema;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SPREAD :
          return new ArrowStructLoader(
              createValueVector( vector , allocator , columnName , ColumnType.SPREAD ) ,
              allocator ,
              schema ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.SPREAD ) ,
              loadSize );
      }
    }

  }

  private static class ArrowBooleanLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowBooleanLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialBooleanLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BOOLEAN ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryBooleanLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BOOLEAN ) ,
              loadSize );
        case CONST :
          return new ArrowConstBooleanLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BOOLEAN ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BOOLEAN ) ,
              loadSize );
      }
    }

  }

  private static class ArrowDoubleLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowDoubleLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialDoubleLoader(
              createValueVector( vector , allocator , columnName , ColumnType.DOUBLE ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryDoubleLoader(
              createValueVector( vector , allocator , columnName , ColumnType.DOUBLE ) ,
              loadSize );
        case CONST :
          return new ArrowConstDoubleLoader(
              createValueVector( vector , allocator , columnName , ColumnType.DOUBLE ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.DOUBLE ) ,
              loadSize );
      }
    }

  }

  private static class ArrowFloatLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowFloatLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialFloatLoader(
              createValueVector( vector , allocator , columnName , ColumnType.FLOAT ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryFloatLoader(
              createValueVector( vector , allocator , columnName , ColumnType.FLOAT ) ,
              loadSize );
        case CONST :
          return new ArrowConstFloatLoader(
              createValueVector( vector , allocator , columnName , ColumnType.FLOAT ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.FLOAT ) ,
              loadSize );
      }
    }

  }

  private static class ArrowByteLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowByteLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialByteLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTE ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryByteLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTE ) ,
              loadSize );
        case CONST :
          return new ArrowConstByteLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTE ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTE ) ,
              loadSize );
      }
    }

  }

  private static class ArrowShortLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowShortLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialShortLoader(
              createValueVector( vector , allocator , columnName , ColumnType.SHORT ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryShortLoader(
              createValueVector( vector , allocator , columnName , ColumnType.SHORT ) ,
              loadSize );
        case CONST :
          return new ArrowConstShortLoader(
              createValueVector( vector , allocator , columnName , ColumnType.SHORT ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.SHORT ) ,
              loadSize );
      }
    }

  }

  private static class ArrowIntegerLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowIntegerLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialIntegerLoader(
              createValueVector( vector , allocator , columnName , ColumnType.INTEGER ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryIntegerLoader(
              createValueVector( vector , allocator , columnName , ColumnType.INTEGER ) ,
              loadSize );
        case CONST :
          return new ArrowConstIntegerLoader(
              createValueVector( vector , allocator , columnName , ColumnType.INTEGER ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.INTEGER ) ,
              loadSize );
      }
    }

  }

  private static class ArrowLongLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowLongLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialLongLoader(
              createValueVector( vector , allocator , columnName , ColumnType.LONG ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryLongLoader(
              createValueVector( vector , allocator , columnName , ColumnType.LONG ) ,
              loadSize );
        case CONST :
          return new ArrowConstLongLoader(
              createValueVector( vector , allocator , columnName , ColumnType.LONG ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.LONG ) ,
              loadSize );
      }
    }

  }

  private static class ArrowStringLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowStringLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialStringLoader(
              createValueVector( vector , allocator , columnName , ColumnType.STRING ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryStringLoader(
              createValueVector( vector , allocator , columnName , ColumnType.STRING ) ,
              loadSize );
        case CONST :
          return new ArrowConstStringLoader(
              createValueVector( vector , allocator , columnName , ColumnType.STRING ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.STRING ) ,
              loadSize );
      }
    }

  }

  private static class ArrowBytesLoaderFactory implements ILoaderFactory<ValueVector> {

    private final ValueVector vector;
    private final BufferAllocator allocator;
    private final String columnName;

    private ArrowBytesLoaderFactory(
        final ValueVector vector ,
        final BufferAllocator allocator ,
        final String columnName ) {
      this.vector = vector;
      this.allocator = allocator;
      this.columnName = columnName;
    }

    @Override
    public ILoader<ValueVector> createLoader(
        final ColumnBinary columnBinary ,
        final int loadSize ) throws IOException {
      switch ( getLoadType( columnBinary , loadSize ) ) {
        case SEQUENTIAL :
          return new ArrowSequentialBytesLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTES ) ,
              loadSize );
        case DICTIONARY :
          return new ArrowDictionaryBytesLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTES ) ,
              loadSize );
        case CONST :
          return new ArrowConstBytesLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTES ) ,
              loadSize );
        default :
          return new ArrowNullLoader(
              createValueVector( vector , allocator , columnName , ColumnType.BYTES ) ,
              loadSize );
      }
    }

  }

  /**
   * Create vector from StructVector.
   */
  public static ValueVector createValueVectorFromStructVector(
      final StructVector vector ,
      final BufferAllocator allocator ,
      final String columnName ,
      final ColumnType columnType ) {
    switch ( columnType ) {
      case UNION:
        return vector.addOrGetUnion( columnName );
      case ARRAY:
        return vector.addOrGetList( columnName );
      case MAP:
      case STRUCT:
      case SPREAD:
        return vector.addOrGetStruct( columnName );
      case BOOLEAN:
        return vector.addOrGet(
            columnName ,
            new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) ,
            BitVector.class );
      case BYTE:
        return vector.addOrGet(
            columnName ,
            new FieldType( true , new ArrowType.Int( 8 , true ) , null , null ) ,
            TinyIntVector.class );  
      case SHORT:
        return vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.Int( 16 , true ) ,
              null ,
              null ) ,
            SmallIntVector.class );
      case INTEGER:
        return vector.addOrGet(
            columnName ,
            new FieldType( true , new ArrowType.Int( 32 , true ) , null , null ) ,
            IntVector.class );
      case LONG:
        return vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.Int( 64 , true ) ,
              null ,
              null ) ,
              BigIntVector.class );
      case FLOAT:
        return vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.SINGLE ) ,
              null ,
              null ) ,
            Float4Vector.class );
      case DOUBLE:
        return vector.addOrGet(
            columnName ,
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) ,
              null ,
              null ) ,
            Float8Vector.class );
      case STRING:
        return vector.addOrGet(
            columnName ,
            new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) ,
            VarCharVector.class );
      case BYTES:
        return vector.addOrGet(
            columnName ,
            new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) ,
            VarBinaryVector.class );

      case NULL:
      default:
        return null;
    }
  }

  /**
   * Create vector from ListVector.
   */
  public static ValueVector createValueVectorFromListVector(
      final ListVector vector ,
      final BufferAllocator allocator ,
      final String columnName ,
      final ColumnType columnType ) {
    switch ( columnType ) {
      case UNION:
        return vector.addOrGetVector(
            new FieldType( true , MinorType.UNION.getType() , null , null ) ).getVector();
      case ARRAY:
        return vector.addOrGetVector(
            new FieldType( true , ArrowType.List.INSTANCE , null , null ) ).getVector();
      case MAP:
      case STRUCT:
      case SPREAD:
        return vector.addOrGetVector(
            new FieldType( true , ArrowType.Struct.INSTANCE , null , null ) ).getVector();
      case BOOLEAN:
        return vector.addOrGetVector(
            new FieldType( true , ArrowType.Bool.INSTANCE , null , null ) ).getVector();
      case BYTE:
        return vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 8 , true ) , null , null ) ).getVector();
      case SHORT:
        return vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 16 , true ) , null , null ) ).getVector();
      case INTEGER:
        return vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 32 , true ) , null , null ) ).getVector();
      case LONG:
        return vector.addOrGetVector(
            new FieldType( true , new ArrowType.Int( 64 , true ) , null , null ) ).getVector();
      case FLOAT:
        return vector.addOrGetVector(
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.HALF ) ,
              null ,
              null ) ).getVector();
      case DOUBLE:
        return vector.addOrGetVector(
            new FieldType(
              true ,
              new ArrowType.FloatingPoint( FloatingPointPrecision.DOUBLE ) ,
              null ,
              null ) ).getVector();
      case STRING:
        return vector.addOrGetVector(
            new FieldType( true , ArrowType.Utf8.INSTANCE , null , null ) ).getVector();
      case BYTES:
        return vector.addOrGetVector(
            new FieldType( true , ArrowType.Binary.INSTANCE , null , null ) ).getVector();

      case NULL:
      default:
        return null;
    }
  }

  /**
   * Create vector from UnionVector.
   */
  public static ValueVector createValueVectorFromUnionVector(
      final UnionVector vector ,
      final BufferAllocator allocator ,
      final String columnName ,
      final ColumnType columnType ) {
    switch ( columnType ) {
      case ARRAY:
        return vector.getList();
      case MAP:
      case STRUCT:
        return vector.getStruct();
      case SPREAD:
        return vector.getStruct();
      case BOOLEAN:
        return vector.getBitVector();
      case BYTE:
        return vector.getTinyIntVector();
      case SHORT:
        return vector.getSmallIntVector();
      case INTEGER:
        return vector.getIntVector();
      case LONG:
        return vector.getBigIntVector();
      case FLOAT:
        return vector.getFloat4Vector();
      case DOUBLE:
        return vector.getFloat8Vector();
      case STRING:
        return vector.getVarCharVector();
      case BYTES:
        return vector.getVarBinaryVector();

      case UNION:
      case NULL:
      default:
        return null;
    }
  }

  /**
   * Create ValueVector from columnType.
   */
  public static ValueVector createValueVector(
      final ValueVector vector ,
      final BufferAllocator allocator ,
      final String columnName ,
      final ColumnType columnType ) {
    if ( vector instanceof StructVector ) {
      return createValueVectorFromStructVector(
          (StructVector)vector , allocator , columnName , columnType );
    } else if ( vector instanceof ListVector ) {
      return createValueVectorFromListVector(
          (ListVector)vector , allocator , columnName , columnType );
    } else if ( vector instanceof UnionVector ) {
      return createValueVectorFromUnionVector(
          (UnionVector)vector , allocator , columnName , columnType );
    } else {
      throw new UnsupportedOperationException(
        "This ValueVector is not supported. "
        + "Struct and List are supported." );
    }
  }

  /**
   * Create loader factory from schema type.
   */
  public static ILoaderFactory<ValueVector> createLoaderFactory(
      final ValueVector vector ,
      final BufferAllocator allocator ,
      final IField schema ) {
    ColumnType columnType;
    switch ( schema.getFieldType() ) {
      case UNION:
        columnType = ColumnType.UNION;
        break;
      case ARRAY:
        columnType = ColumnType.ARRAY;
        break;
      case MAP:
        columnType = ColumnType.SPREAD;
        break;
      case STRUCT:
        columnType = ColumnType.SPREAD;
        break;
      case BOOLEAN:
        columnType = ColumnType.BOOLEAN;
        break;
      case BYTE:
        columnType = ColumnType.BYTE;
        break;
      case BYTES:
        columnType = ColumnType.BYTES;
        break;
      case DOUBLE:
        columnType = ColumnType.DOUBLE;
        break;
      case FLOAT:
        columnType = ColumnType.FLOAT;
        break;
      case INTEGER:
        columnType = ColumnType.INTEGER;
        break;
      case LONG:
        columnType = ColumnType.LONG;
        break;
      case SHORT:
        columnType = ColumnType.SHORT;
        break;
      case STRING:
        columnType = ColumnType.STRING;
        break;
      case NULL:
      default:
        columnType = ColumnType.NULL;
    }
    return createLoaderFactory( vector , allocator , schema.getName() , schema , columnType );
  }

  /**
   * Create loader factory from column type.
   */
  public static ILoaderFactory<ValueVector> createLoaderFactory(
      final ValueVector vector ,
      final BufferAllocator allocator ,
      final String columnName ,
      final IField schema ,
      final ColumnType columnType ) {
    switch ( columnType ) {
      case UNION:
        return new ArrowUnionLoaderFactory(
            vector , allocator , columnName , schema );
      case ARRAY:
        return new ArrowArrayLoaderFactory(
            vector , allocator , columnName , schema );
      case MAP:
        return new ArrowMapLoaderFactory(
            vector , allocator , columnName , schema );
      case STRUCT:
      case SPREAD:
        return new ArrowSpreadLoaderFactory(
            vector , allocator , columnName , schema );

      case BOOLEAN:
        return new ArrowBooleanLoaderFactory(
            vector , allocator , columnName );

      case DOUBLE:
        return new ArrowDoubleLoaderFactory(
            vector , allocator , columnName );
      case FLOAT:
        return new ArrowFloatLoaderFactory(
            vector , allocator , columnName );

      case BYTE:
        return new ArrowByteLoaderFactory(
            vector , allocator , columnName );
      case SHORT:
        return new ArrowShortLoaderFactory(
            vector , allocator , columnName );
      case INTEGER:
        return new ArrowIntegerLoaderFactory(
            vector , allocator , columnName );
      case LONG:
        return new ArrowLongLoaderFactory(
            vector , allocator , columnName );

      case STRING:
        return new ArrowStringLoaderFactory(
            vector , allocator , columnName );
      case BYTES:
        return new ArrowBytesLoaderFactory(
            vector , allocator , columnName );

      case NULL:
      default:
        throw new UnsupportedOperationException(
            "This load type is not supported :" + columnType );
    }
  }

}
