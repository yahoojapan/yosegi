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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressResult;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import jp.co.yahoo.yosegi.inmemory.YosegiLoaderFactory;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ColumnTypeFactory;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.UnionColumn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class DumpUnionColumnBinaryMaker implements IColumnBinaryMaker {

  public enum MargeType {
    INTEGER,
    FLOAT,
    MIX
  }

  /**
   * Select a merge type from the column.
   */
  public static MargeType getMargeType( final ColumnType columnType ) {
    switch ( columnType ) {
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
        return MargeType.INTEGER;
      case FLOAT:
      case DOUBLE:
        return MargeType.FLOAT;
      default:
        return MargeType.MIX;
    }
  }

  private MargeType checkSameAllColumnType(
      final List<IColumn> columnList , final MargeType mergeType ) {
    for ( IColumn column : columnList ) {
      if ( getMargeType( column.getColumnType() ) != mergeType ) {
        return MargeType.MIX;
      }
    }
    return mergeType;
  }

  /**
   * Select a merge type from the child column list.
   */
  public MargeType checkMargeType( final List<IColumn> columnList ) {
    switch ( getMargeType( columnList.get(0).getColumnType() ) ) {
      case INTEGER:
        return checkSameAllColumnType( columnList , MargeType.INTEGER );
      case FLOAT:
        return checkSameAllColumnType( columnList , MargeType.FLOAT );
      default:
        return MargeType.MIX;
    }
  }

  private ColumnBinary mergeColumn(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ,
      final List<IColumn> childColumnList ) throws IOException {
    int max = -1;
    IColumnBinaryMaker maker = null;
    ColumnType type = null;
    for ( IColumn childColumn : childColumnList ) {
      ColumnType columnType = childColumn.getColumnType();
      int columnSize = ColumnTypeFactory.getColumnTypeToPrimitiveByteSize( columnType , null );
      if ( max < columnSize ) {
        max = columnSize;
        maker = commonConfig.getColumnMaker( columnType );
        type = columnType;
        if ( currentConfigNode != null ) {
          maker = currentConfigNode.getCurrentConfig().getColumnMaker( columnType );
        }
      }
    }

    PrimitiveColumn primitiveColumn = new PrimitiveColumn( type , column.getColumnName() );
    primitiveColumn.setCellManager( column.getCellManager() );

    return maker.toBinary(
        commonConfig , currentConfigNode , compressResultNode , primitiveColumn );
  }

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final CompressResultNode compressResultNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }
    List<IColumn> childColumnList = column.getListColumn();
    MargeType mergeType = checkMargeType( childColumnList );
    if ( mergeType != MargeType.MIX ) {
      return mergeColumn(
          commonConfig , currentConfigNode , compressResultNode , column , childColumnList );
    }
    List<ColumnBinary> columnBinaryList = new ArrayList<ColumnBinary>();
    for ( IColumn childColumn : childColumnList ) {
      ColumnBinaryMakerCustomConfigNode childNode = null;
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( childColumn.getColumnType() );
      if ( currentConfigNode != null ) {
        childNode = currentConfigNode.getChildConfigNode( childColumn.getColumnName() );
        if ( childNode != null ) {
          maker = childNode.getCurrentConfig().getColumnMaker( childColumn.getColumnType() );
        }
      }
      columnBinaryList.add( maker.toBinary(
          commonConfig ,
          childNode ,
          compressResultNode.getChild( childColumn.getColumnName() ) ,
          childColumn ) );
    }

    byte[] rawBinary = new byte[ column.size() ];
    ByteBuffer wrapBuffer = ByteBuffer.wrap( rawBinary );
    for ( int i = 0 ; i < column.size() ; i++ ) {
      wrapBuffer.put( ColumnTypeFactory.getColumnTypeByte( column.get(i).getType() ) );
    }

    CompressResult compressResult = compressResultNode.getCompressResult(
        this.getClass().getName() ,
        "c0"  ,
        currentConfig.compressionPolicy ,
        currentConfig.allowedRatio );
    byte[] compressData =
        currentConfig.compressorClass.compress( rawBinary , 0 , rawBinary.length , compressResult );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.UNION ,
        column.size() ,
        rawBinary.length ,
        column.size() * Byte.BYTES , -1 ,
        compressData ,
        0 ,
        compressData.length ,
        columnBinaryList );
  }

  @Override
  public int calcBinarySize( final IColumnAnalizeResult analizeResult ) {
    return analizeResult.getColumnSize();
  }

  @Override
  public IColumn toColumn( final ColumnBinary columnBinary ) throws IOException {
    return new LazyColumn(
        columnBinary.columnName ,
        columnBinary.columnType ,
        new UnionColumnManager( columnBinary ) );
  }

  @Override
  public LoadType getLoadType( final ColumnBinary columnBinary , final int loadSize ) {
    return LoadType.UNION;
  }

  @Override
  public void load(
      final ColumnBinary columnBinary , final ILoader loader ) throws IOException {
    if ( loader.getLoaderType() != LoadType.UNION ) {
      throw new IOException( "Loader type is not UNION." );
    }
    IUnionLoader unionLoader = (IUnionLoader)loader;
    for ( ColumnBinary child : columnBinary.columnBinaryList ) {
      unionLoader.loadChild( child , loader.getLoadSize() );
    }

    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] cellBinary = compressor.decompress(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    ByteBuffer wrapBuffer = ByteBuffer.wrap( cellBinary );

    if ( columnBinary.loadIndex == null ) {
      for ( int i = 0 ; i < cellBinary.length ; i++ ) {
        ColumnType columnType = ColumnTypeFactory.getColumnTypeFromByte( wrapBuffer.get() );
        unionLoader.setIndexAndColumnType( i , columnType );
      }
      for ( int i = cellBinary.length ; i < loader.getLoadSize() ; i++ ) {
        unionLoader.setNull( i );
      }
    } else {
      int currentIndex = 0;
      for ( int index : columnBinary.loadIndex ) {
        if ( cellBinary.length <= index ) {
          unionLoader.setNull( currentIndex );
        } else {
          ColumnType columnType = ColumnTypeFactory.getColumnTypeFromByte(
              wrapBuffer.get( index ) );
          if ( columnType == ColumnType.NULL ) {
            unionLoader.setNull( currentIndex );
          } else {
            unionLoader.setIndexAndColumnType( currentIndex , columnType );
          }
        }
        currentIndex++;
      }
    }

    unionLoader.finish();
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    int maxValueCount = 0;
    for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
      IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
      IMemoryAllocator childAllocator =
          allocator.getChild( childColumnBinary.columnName , childColumnBinary.columnType );
      maker.loadInMemoryStorage( childColumnBinary , childAllocator );
      if ( maxValueCount < childAllocator.getValueCount() ) {
        maxValueCount = childAllocator.getValueCount();
      }
    }
    allocator.setValueCount( maxValueCount );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class UnionColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private UnionColumn unionColumn;
    private boolean isCreate;

    public UnionColumnManager( final ColumnBinary columnBinary ) {
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }

      Map<ColumnType,IColumn> columnContainer = new EnumMap<>( ColumnType.class );
      unionColumn = new UnionColumn( columnBinary.columnName , columnContainer );

      for ( ColumnBinary childColumnBinary : columnBinary.columnBinaryList ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( childColumnBinary.makerClassName );
        IColumn column = maker.toColumn( childColumnBinary );
        column.setParentsColumn( unionColumn );
        unionColumn.setColumn( column );
        columnContainer.put( column.getColumnType() , column );
      }


      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] cellBinary = compressor.decompress(
          columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
      ByteBuffer wrapBuffer = ByteBuffer.wrap( cellBinary );
      for ( int i = 0 ; i < cellBinary.length ; i++ ) {
        ColumnType columnType = ColumnTypeFactory.getColumnTypeFromByte( wrapBuffer.get() );
        if ( columnContainer.containsKey( columnType ) ) {
          unionColumn.addCell( columnType , columnContainer.get( columnType ).get( i ) , i );
        }
      }

      isCreate = true;
    }

    @Override
    public IColumn get() {
      try {
        create();
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
      }
      return unionColumn;
    }

    @Override
    public List<String> getColumnKeys() {
      return new ArrayList<>();
    }

    @Override
    public int getColumnSize() {
      return 0;
    }
  }

}
