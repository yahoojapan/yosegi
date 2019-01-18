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
import jp.co.yahoo.yosegi.binary.maker.index.SequentialBooleanCellIndex;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.constants.PrimitiveByteLength;
import jp.co.yahoo.yosegi.inmemory.IMemoryAllocator;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.ICell;
import jp.co.yahoo.yosegi.spread.column.ICellManager;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.PrimitiveCell;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.filter.IFilter;
import jp.co.yahoo.yosegi.spread.column.index.DefaultCellIndex;
import jp.co.yahoo.yosegi.spread.column.index.ICellIndex;
import jp.co.yahoo.yosegi.spread.expression.IExpressionIndex;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class DumpBooleanColumnBinaryMaker implements IColumnBinaryMaker {

  private static final BooleanObj TRUE = new BooleanObj( true );
  private static final BooleanObj FALSE = new BooleanObj( false );

  @Override
  public ColumnBinary toBinary(
      final ColumnBinaryMakerConfig commonConfig ,
      final ColumnBinaryMakerCustomConfigNode currentConfigNode ,
      final IColumn column ) throws IOException {
    ColumnBinaryMakerConfig currentConfig = commonConfig;
    if ( currentConfigNode != null ) {
      currentConfig = currentConfigNode.getCurrentConfig();
    }

    byte[] binary = new byte[ column.size() ];
    int rowCount = 0;
    for ( int i = 0 ; i < column.size() ; i++ ) {
      ICell cell = column.get(i);
      if ( cell.getType() == ColumnType.NULL ) {
        binary[i] = (byte)2;
      } else if ( ( (PrimitiveCell)cell ).getRow().getBoolean() ) {
        rowCount++;
        binary[i] = (byte)1;
      } else {
        rowCount++;
        binary[i] = (byte)0;
      }
    }

    byte[] compressData = currentConfig.compressorClass.compress( binary , 0 , binary.length );

    return new ColumnBinary(
        this.getClass().getName() ,
        currentConfig.compressorClass.getClass().getName() ,
        column.getColumnName() ,
        ColumnType.BOOLEAN ,
        rowCount ,
        binary.length ,
        rowCount * PrimitiveByteLength.BOOLEAN_LENGTH ,
        -1 ,
        compressData ,
        0 ,
        compressData.length ,
        null );
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
        new BooleanColumnManager( columnBinary ) );
  }

  @Override
  public void loadInMemoryStorage(
      final ColumnBinary columnBinary ,
      final IMemoryAllocator allocator ) throws IOException {
    ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
    byte[] binary = compressor.decompress(
        columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );
    for ( int i = 0 ; i < binary.length ; i++ ) {
      if ( binary[i] == (byte)0 ) {
        allocator.setBoolean( i , false );
      } else if ( binary[i] == (byte)1 ) {
        allocator.setBoolean( i , true );
      } else {
        allocator.setNull( i );
      }
    }
    allocator.setValueCount( binary.length );
  }

  @Override
  public void setBlockIndexNode(
      final BlockIndexNode parentNode ,
      final ColumnBinary columnBinary ,
      final int spreadIndex ) throws IOException {
    parentNode.getChildNode( columnBinary.columnName ).disable();
  }

  public class DirectBufferBooleanCellManager implements ICellManager {

    private final PrimitiveCell[] cellArray;
    private byte[] buffer;

    private ICellIndex index = new DefaultCellIndex();

    /**
     * Manage byte array as Boolean cell.
     */
    public DirectBufferBooleanCellManager(
        final byte[] buffer , final PrimitiveObject trueObj , final PrimitiveObject falseObj ) {
      this.buffer = buffer;
      cellArray = new PrimitiveCell[]{
          new PrimitiveCell( ColumnType.BOOLEAN , falseObj ) ,
          new PrimitiveCell( ColumnType.BOOLEAN , trueObj ) , null };
    }

    @Override
    public void add( final ICell cell , final int index ) {
      throw new UnsupportedOperationException( "read only." );
    }

    @Override
    public ICell get( final int index , final ICell defaultCell ) {
      if ( buffer.length <= index ) {
        return defaultCell;
      }
      byte targetCellIndex = buffer[index];
      if ( targetCellIndex == Byte.MAX_VALUE ) {
        targetCellIndex = (byte)2;
      }
      ICell result = cellArray[targetCellIndex];

      if ( result == null ) {
        return defaultCell;
      }
      return result;
    }

    @Override
    public int size() {
      return buffer.length;
    }

    @Override
    public void clear() {
      buffer = new byte[0];
    }

    @Override
    public void setIndex( final ICellIndex index ) {
      this.index = index;
    }

    @Override
    public boolean[] filter(
        final IFilter filter , final boolean[] filterArray ) throws IOException {
      switch ( filter.getFilterType() ) {
        case NOT_NULL:
          return null;
        case NULL:
          return null;
        default:
          return index.filter( filter , filterArray );
      }
    }

    @Override
    public PrimitiveObject[] getPrimitiveObjectArray(
        final IExpressionIndex indexList ,
        final int start ,
        final int length ) {
      PrimitiveObject[] result = new PrimitiveObject[length];
      for ( int i = start , index = 0 ; i < buffer.length && i < ( start + length ); i++,index++ ) {
        int targetIndex = indexList.get(i);
        int cellIndex = buffer[targetIndex];
        if ( cellIndex == Byte.MAX_VALUE ) {
          cellIndex = 2;
        }
        PrimitiveCell cell = cellArray[cellIndex];
        if ( cell != null ) {
          result[index] = cell.getRow();
        }
      }
      return result;
    }

    @Override
    public void setPrimitiveObjectArray(
        final IExpressionIndex indexList ,
        final int start ,
        final int length ,
        final IMemoryAllocator allocator ) {
      int index = 0;
      for ( int i = start ; i < buffer.length && i < ( start + length ); i++,index++ ) {
        int targetIndex = indexList.get(i);
        int cellIndex = buffer[targetIndex];
        try {
          if ( cellIndex == Byte.MAX_VALUE ) {
            allocator.setNull( index );
          } else if ( cellIndex == (byte)1 ) {
            allocator.setPrimitiveObject( index , TRUE );
          } else {
            allocator.setPrimitiveObject( index , FALSE );
          }
        } catch ( IOException ex ) {
          throw new RuntimeException( ex );
        }
      }
      for ( int i = index ; i < length ; i++ ) {
        allocator.setNull( i );
      }
    }

  }

  public class BooleanColumnManager implements IColumnManager {

    private final ColumnBinary columnBinary;
    private PrimitiveColumn column;
    private boolean isCreate;

    public BooleanColumnManager( final ColumnBinary columnBinary ) throws IOException {
      this.columnBinary = columnBinary;
    }

    private void create() throws IOException {
      if ( isCreate ) {
        return;
      }

      ICompressor compressor = FindCompressor.get( columnBinary.compressorClassName );
      byte[] binary = compressor.decompress(
          columnBinary.binary , columnBinary.binaryStart , columnBinary.binaryLength );

      PrimitiveObject trueObject = TRUE;
      PrimitiveObject falseObject = FALSE;

      column = new PrimitiveColumn( ColumnType.BOOLEAN , columnBinary.columnName );
      column.setCellManager(
          new DirectBufferBooleanCellManager( binary , trueObject , falseObject ) );
      column.setIndex( new SequentialBooleanCellIndex( binary ) );

      isCreate = true;
    }

    @Override
    public IColumn get() {
      try {
        create();
      } catch ( IOException ex ) {
        throw new UncheckedIOException( ex );
      }
      return column;
    }

    @Override
    public List<String> getColumnKeys() {
      return new ArrayList<String>();
    }

    @Override
    public int getColumnSize() {
      return 0;
    }
  }

}
