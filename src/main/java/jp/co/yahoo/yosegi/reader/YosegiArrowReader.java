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

package jp.co.yahoo.yosegi.reader;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.design.StructContainerField;
import jp.co.yahoo.yosegi.spread.expression.IExpressionNode;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

public class YosegiArrowReader {

  private final IArrowLoader arrowLoader;

  /**
   * Create new instance.
   */
  public static YosegiArrowReader newInstance( final String localFilePath ) throws IOException {
    return newInstance( new File( localFilePath ) );
  }

  /**
   * Create new instance.
   */
  public static YosegiArrowReader newInstance(
      final String localFilePath , final Configuration config ) throws IOException {
    return newInstance( new File( localFilePath ) , config );
  }

  /**
   * Create new instance.
   */
  public static YosegiArrowReader newInstance(
      final File localFile ) throws IOException {
    return newInstance( localFile , new Configuration() );
  }

  /**
   * Create new instance.
   */
  public static YosegiArrowReader newInstance(
      final File localFile , final Configuration config ) throws IOException {
    return newInstance( new FileInputStream( localFile ) , localFile.length() , config );
  }

  /**
   * Create new instance.
   */
  public static YosegiArrowReader newInstance(
      final InputStream in , final long length , final Configuration config ) throws IOException {
    YosegiReader reader = new YosegiReader();
    reader.setNewStream( in , length , config );
    return new YosegiArrowReader( reader , config );
  }

  /**
   * Initialize without schema definition.
   */
  public YosegiArrowReader(
      final YosegiReader reader , final Configuration config ) throws IOException {
    IRootMemoryAllocator rootAllocator = new DynamicSchemaRootMemoryAllocator();
    BufferAllocator allocator = new RootAllocator( Integer.MAX_VALUE );
    if ( config.containsKey( "spread.reader.expand.column" )
        || config.containsKey( "spread.reader.flatten.column" ) ) {
      arrowLoader = new DynamicArrowLoader( rootAllocator , reader , allocator );
    } else {
      arrowLoader = new DirectArrowLoader( rootAllocator , reader , allocator );
    }
  }

  /**
   * Perform schema definition and initialize.
   */
  public YosegiArrowReader(
      final StructContainerField schema ,
      final YosegiReader reader ,
      final Configuration config ) throws IOException {
    IRootMemoryAllocator rootAllocator = new FixedSchemaRootMemoryAllocator( schema );
    BufferAllocator allocator = new RootAllocator( Integer.MAX_VALUE );
    if ( config.containsKey( "spread.reader.expand.column" )
        || config.containsKey( "spread.reader.flatten.column" ) ) {
      arrowLoader = new DynamicArrowLoader( rootAllocator , reader , allocator );
    } else {
      arrowLoader = new DirectArrowLoader( rootAllocator , reader , allocator );
    }
  }

  public void setNode( final IExpressionNode node ) {
    arrowLoader.setNode( node );
  }

  public boolean hasNext() throws IOException {
    return arrowLoader.hasNext();
  }

  public ValueVector next() throws IOException {
    return arrowLoader.next();
  }

  /**
   * Read next.
   */
  public VectorSchemaRoot nextToSchemaRoot() throws IOException {
    return new VectorSchemaRoot( (FieldVector)next() );
  }

  /**
   * Read next.
   */
  public byte[] nextToBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    VectorSchemaRoot schemaRoot = nextToSchemaRoot();
    ArrowFileWriter writer = new ArrowFileWriter( schemaRoot, null, Channels.newChannel( out ) );
    writer.start();
    writer.writeBatch();
    writer.end();
    writer.close();
    return out.toByteArray();
  }

  public void close() throws IOException {
    arrowLoader.close();
  }

}
