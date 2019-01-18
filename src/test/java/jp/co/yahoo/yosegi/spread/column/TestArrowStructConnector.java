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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.util.List;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.writer.YosegiWriter;
import jp.co.yahoo.yosegi.reader.YosegiReader;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import jp.co.yahoo.yosegi.reader.YosegiArrowReader;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.ArrowSpreadUtil;
import jp.co.yahoo.yosegi.spread.column.IColumn;

public class TestArrowStructConnector{

  private byte[] createTestFile() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "spread/column/struct_test_data.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.addParserRow( parser );
      line = in.readLine();
    }
    writer.close();
    return out.toByteArray();
  }

  @Test
  public void T_1() throws IOException{
    byte[] yosegiFile = createTestFile();
    InputStream in = new ByteArrayInputStream( yosegiFile );
    YosegiReader reader = new YosegiReader();
    Configuration config = new Configuration();
    reader.setNewStream( in , yosegiFile.length , config );
    YosegiArrowReader arrowReader = new YosegiArrowReader( reader , config );

    File file = new File( "target/struct_column_test.arrow" );
    if( file.exists() ){
      file.delete();
    }
    FileOutputStream out = new FileOutputStream( file );
    ArrowFileWriter writer = null;
    while( arrowReader.hasNext() ){
      ValueVector vector = arrowReader.next();
      if( writer == null ){
        VectorSchemaRoot schema = new VectorSchemaRoot( (FieldVector)vector );
        writer = new ArrowFileWriter( schema, null, out.getChannel() );
        writer.start();
      }
      writer.writeBatch();
    }
    writer.end();
    writer.close();
    reader.close();

    ArrowFileReader  ar = new ArrowFileReader( new FileInputStream( "target/struct_column_test.arrow" ).getChannel() ,  new RootAllocator( Integer.MAX_VALUE ) );
    VectorSchemaRoot root = ar.getVectorSchemaRoot();
    ArrowBlock rbBlock = ar.getRecordBlocks().get(0);
    ar.loadRecordBatch(rbBlock);
    List<FieldVector> fieldVectorList = root.getFieldVectors();
    Spread spread = ArrowSpreadUtil.toSpread( root.getRowCount() , fieldVectorList );

    IColumn spreadColumn = spread.getColumn( "s" );
    assertEquals( spreadColumn.getColumnSize() , 2 );
    assertEquals( spreadColumn.size() , 6 );

    IColumn numColumn = spreadColumn.getColumn( "num" );
    assertEquals( ( (PrimitiveObject)( numColumn.get(0).getRow() ) ).getInt() , 0 );
    assertEquals( numColumn.get(1).getRow() , null );
    assertEquals( ( (PrimitiveObject)( numColumn.get(2).getRow() ) ).getInt() , 2 );
    assertEquals( ( (PrimitiveObject)( numColumn.get(3).getRow() ) ).getInt() , 3 );
    assertEquals( ( (PrimitiveObject)( numColumn.get(4).getRow() ) ).getInt() , 4 );
    assertEquals( ( (PrimitiveObject)( numColumn.get(5).getRow() ) ).getInt() , 5 );

    IColumn strColumn = spreadColumn.getColumn( "str" );
    assertEquals( ( (PrimitiveObject)( strColumn.get(0).getRow() ) ).getString() , "a" );
    assertEquals( ( (PrimitiveObject)( strColumn.get(1).getRow() ) ).getString() , "b" );
    assertEquals( strColumn.get(2).getRow() , null );
    assertEquals( ( (PrimitiveObject)( strColumn.get(3).getRow() ) ).getString() , "d" );
    assertEquals( ( (PrimitiveObject)( strColumn.get(4).getRow() ) ).getString() , "e" );
    assertEquals( ( (PrimitiveObject)( strColumn.get(5).getRow() ) ).getString() , "f" );

    file.delete();
  }

}
