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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.CompressResultNode;
import jp.co.yahoo.yosegi.binary.FindColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.optimizer.BinaryMakerOptimizer;
import jp.co.yahoo.yosegi.binary.optimizer.FindOptimizerFactory;
import jp.co.yahoo.yosegi.binary.optimizer.IOptimizerFactory;
import jp.co.yahoo.yosegi.blockindex.BlockIndexNode;
import jp.co.yahoo.yosegi.compressor.CompressorNameShortCut;
import jp.co.yahoo.yosegi.compressor.DefaultCompressor;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.analyzer.Analyzer;
import jp.co.yahoo.yosegi.spread.analyzer.IColumnAnalizeResult;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PushdownSupportedBlockWriter implements IBlockWriter {

  private static final int META_BUFFER_SIZE = 1024 * 1024 * 1;

  private final List<Integer> spreadSizeList = new ArrayList<Integer>();
  private final BlockIndexNode blockIndexNode = new BlockIndexNode();

  private ColumnBinaryMakerCustomConfigNode configNode;
  private CompressResultNode compressResultNode;
  private ByteArrayData metaBuffer;
  private int blockSize;
  private ColumnBinaryTree columnTree;
  private boolean makeCustomConfig;
  private IOptimizerFactory optimizerFactory;
  private ICompressor compressor;
  private byte[] compressorClassNameBytes;

  private byte[] headerBytes;
  private int bufferSize;

  /**
   * Define the required initial value.
   */
  public PushdownSupportedBlockWriter() throws IOException {
    compressor = new DefaultCompressor();
    compressorClassNameBytes = CompressorNameShortCut.getShortCutName(
        compressor.getClass().getName() ).getBytes( "UTF-8" );
  }

  @Override
  public void setup( final int blockSize , final Configuration config ) throws IOException {
    this.blockSize = blockSize;
    spreadSizeList.clear();

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    if ( config.containsKey( "spread.column.maker.default.compress.class" ) ) {
      defaultConfig.compressorClass =
          FindCompressor.get( config.get( "spread.column.maker.default.compress.class" ) );
    }
    if ( config.containsKey( "compress.optimize.allowed.ratio" ) ) {
      double allowedRatio = config.getDouble( "compress.optimize.allowed.ratio" , 1.25d );
      if ( 0 < Double.valueOf( allowedRatio ).compareTo( 0d ) ) {
        defaultConfig.allowedRatio = allowedRatio;
      }
    }

    if ( config.containsKey( "spread.column.maker.setting" ) ) {
      JacksonMessageReader jsonReader = new JacksonMessageReader();
      IParser jsonParser = jsonReader.create( config.get( "spread.column.maker.setting" ) );
      configNode = new ColumnBinaryMakerCustomConfigNode( defaultConfig , jsonParser );
    } else if ( config.get( "spread.column.maker.use.auto.optimizer" , "true" ).equals( "true" ) ) {
      makeCustomConfig = true;
      optimizerFactory = FindOptimizerFactory.get( config.get(
          "spread.column.maker.use.auto.optimizer.factory.class" ,
          "jp.co.yahoo.yosegi.binary.optimizer.DefaultOptimizerFactory" ) , config );
      configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    } else {
      configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    }
    compressResultNode = new CompressResultNode();

    metaBuffer = new ByteArrayData( META_BUFFER_SIZE );
    columnTree = new ColumnBinaryTree();

    bufferSize = 0;
    headerBytes = new byte[0];
    compressor = FindCompressor.get( config.get( 
        "block.maker.compress.class" ,
        "jp.co.yahoo.yosegi.compressor.DefaultCompressor" ) );
    compressorClassNameBytes = CompressorNameShortCut.getShortCutName(
        compressor.getClass().getName() ).getBytes( "UTF-8" );
  }

  @Override
  public void appendHeader( final byte[] headerBytes ) {
    if ( this.headerBytes.length == 0 ) {
      this.headerBytes = headerBytes;
    } else {
      byte[] mergeByte = new byte[ this.headerBytes.length + headerBytes.length ];
      ByteBuffer wrapBuffer = ByteBuffer.wrap( mergeByte );
      wrapBuffer.put( this.headerBytes );
      wrapBuffer.put( headerBytes );
      this.headerBytes = mergeByte;
    }
  }

  private int getColumnBinarySize( final List<ColumnBinary> binaryList ) throws IOException {
    int length = 0;
    for ( ColumnBinary binary : binaryList ) {
      length += binary.size();
    }
    return length;
  }

  @Override
  public void append(
        final int spreadSize , final List<ColumnBinary> binaryList ) throws IOException {
    for ( ColumnBinary columnBinary : binaryList ) {
      if ( columnBinary != null ) {
        IColumnBinaryMaker maker = FindColumnBinaryMaker.get( columnBinary.makerClassName );
        maker.setBlockIndexNode( blockIndexNode , columnBinary , getRegisterSpreadCount() );
      }
    }
    bufferSize += getColumnBinarySize( binaryList ) + Integer.BYTES * 2;
    spreadSizeList.add( spreadSize );

    columnTree.addChild( binaryList );
    if ( blockSize <= size() ) {
      throw new IOException( "Buffer overflow." );
    }
  }

  @Override
  public List<ColumnBinary> convertRow( final Spread spread ) throws IOException {
    if ( makeCustomConfig ) {
      List<IColumnAnalizeResult> analizeResultList = Analyzer.analize( spread );
      BinaryMakerOptimizer optimizer = new BinaryMakerOptimizer( analizeResultList );
      configNode = optimizer.createConfigNode( configNode.getCurrentConfig() , optimizerFactory );
      makeCustomConfig = false;
    }
    List<ColumnBinary> result = new ArrayList<ColumnBinary>();
    for ( int i = 0 ; i < spread.getColumnSize() ; i++ ) {
      IColumn column = spread.getColumn( i );
      ColumnBinaryMakerConfig commonConfig = configNode.getCurrentConfig();
      ColumnBinaryMakerCustomConfigNode childConfigNode =
          configNode.getChildConfigNode( column.getColumnName() );
      IColumnBinaryMaker maker = commonConfig.getColumnMaker( column.getColumnType() );
      if ( childConfigNode != null ) {
        maker = childConfigNode.getCurrentConfig().getColumnMaker( column.getColumnType() );
      }
      result.add( maker.toBinary(
          commonConfig ,
          childConfigNode ,
          compressResultNode.getChild( column.getColumnName() ) ,
          column ) );
    }
    return result;
  }

  @Override
  public boolean canAppend( final List<ColumnBinary> binaryList ) throws IOException {
    int length = getColumnBinarySize( binaryList );
    int currentSize = 
        size()
        + length
        + Integer.BYTES
        + Integer.BYTES
        + ( Integer.BYTES * spreadSizeList.size() + 1 );
    return currentSize < blockSize;
  }

  @Override
  public int size() {
    try {
      return
          headerBytes.length
          + bufferSize
          + META_BUFFER_SIZE
          + Integer.BYTES
          + compressorClassNameBytes.length
          + blockIndexNode.getBinarySize()
          + Integer.BYTES;
    } catch ( IOException ex ) {
      throw new RuntimeException( ex );
    }
  }

  @Override
  public void writeFixedBlock( final OutputStream out ) throws IOException {
    write( out , blockSize );
  }

  @Override
  public void writeVariableBlock( final OutputStream out ) throws IOException {
    write( out , -1 );
  }

  @Override
  public void write( final OutputStream out , final int dataSize ) throws IOException {
    byte[] blockIndexBinary = new byte[
        Integer.BYTES   
        + ( compressor.getClass().getName().length() * 2 )
        + blockIndexNode.getBinarySize()
        + Integer.BYTES ];
    ByteBuffer blockIndexBuffer = ByteBuffer.wrap( blockIndexBinary );
    blockIndexBuffer.putInt( compressorClassNameBytes.length );
    blockIndexBuffer.put( compressorClassNameBytes );
    int metaLength = 4 + compressorClassNameBytes.length + 4;
    blockIndexBuffer.putInt( blockIndexBinary.length - metaLength );
    blockIndexNode.toBinary( blockIndexBinary , metaLength );
    appendHeader( blockIndexBinary );
    blockIndexNode.clear();

    int offset = 0;
    out.write( headerBytes , 0 , headerBytes.length );
    offset += headerBytes.length;


    ByteBuffer blockMetaBuffer = ByteBuffer.allocate(
        Integer.BYTES + ( Integer.BYTES * spreadSizeList.size() ) + Integer.BYTES );
    blockMetaBuffer.putInt( spreadSizeList.size() );

    offset += Integer.BYTES;
    for ( Integer spreadSize : spreadSizeList ) {
      blockMetaBuffer.putInt( spreadSize.intValue() );
      offset += Integer.BYTES;
    }

    columnTree.createMeta( metaBuffer , 0 );
    byte[] metaBinary = compressor.compress( metaBuffer.getBytes() , 0 , metaBuffer.getLength() );
    blockMetaBuffer.putInt( metaBinary.length );
    offset += Integer.BYTES;

    out.write( blockMetaBuffer.array() );

    out.write( metaBinary , 0 , metaBinary.length );
    offset += metaBinary.length;
    offset += columnTree.writeData( out );
    if ( dataSize != -1 ) {
      int nullLength = dataSize - offset;
      out.write( new byte[nullLength] );
    } 

    spreadSizeList.clear();
    metaBuffer.clear();
    columnTree.clear();
    headerBytes = new byte[0];
    bufferSize = 0;
  }

  @Override
  public String getReaderClassName() {
    return PushdownSupportedBlockReader.class.getName();
  }

  @Override
  public void close() throws IOException {
    spreadSizeList.clear();
    metaBuffer.clear();
    columnTree.clear();
    bufferSize = 0;
  }

  private int getRegisterSpreadCount() {
    return spreadSizeList.size();
  }

}
