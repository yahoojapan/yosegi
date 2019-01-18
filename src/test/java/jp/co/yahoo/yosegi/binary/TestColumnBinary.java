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
package jp.co.yahoo.yosegi.binary;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestColumnBinary{

  @Test
  public void T_newInstance_1() throws IOException{
    ColumnBinary columnBinary = new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null );
    assertEquals( columnBinary.makerClassName , "hoge.class" );
    assertEquals( columnBinary.compressorClassName , "compressor.class" );
    assertEquals( columnBinary.columnName , "test" );
    assertEquals( columnBinary.columnType , ColumnType.UNKNOWN );
    assertEquals( columnBinary.rowCount , 100 );
    assertEquals( columnBinary.rawDataSize , 1024 );
    assertEquals( Arrays.equals( columnBinary.binary , new byte[100] ) , true );
    assertEquals( columnBinary.binaryStart , 10 );
    assertEquals( columnBinary.binaryLength , 90 );
    assertEquals( columnBinary.columnBinaryList , null );

    assertTrue( 0 < columnBinary.size() );
    assertTrue( 0 < columnBinary.getMetaSize() );
  }

  @Test
  public void T_newInstance_2() throws IOException{
    List<ColumnBinary> childList = new ArrayList();
    ColumnBinary childColumnBinary = new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null );
    childList.add( childColumnBinary );
    ColumnBinary parentColumnBinary = new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , childList );
    ColumnBinary columnBinary = parentColumnBinary.columnBinaryList.get(0);

    assertEquals( columnBinary.makerClassName , "hoge.class" );
    assertEquals( columnBinary.compressorClassName , "compressor.class" );
    assertEquals( columnBinary.columnName , "test" );
    assertEquals( columnBinary.columnType , ColumnType.UNKNOWN );
    assertEquals( columnBinary.rowCount , 100 );
    assertEquals( columnBinary.rawDataSize , 1024 );
    assertEquals( Arrays.equals( columnBinary.binary , new byte[100] ) , true );
    assertEquals( columnBinary.binaryStart , 10 );
    assertEquals( columnBinary.binaryLength , 90 );
    assertEquals( columnBinary.columnBinaryList , null );

    assertTrue( 0 < parentColumnBinary.size() );
    assertTrue( 0 < parentColumnBinary.getMetaSize() );
  }

  @Test
  public void T_toMetaBinary_1() throws IOException{
    byte[] data = new byte[100];
    ColumnBinary originalColumnBinary = new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , data , 10 , 90 , null );
    byte[] metaBinary = originalColumnBinary.toMetaBinary();
    ColumnBinary columnBinary = ColumnBinary.newInstanceFromMetaBinary( metaBinary , 0 , metaBinary.length , data , null );

    assertEquals( columnBinary.makerClassName , "hoge.class" );
    assertEquals( columnBinary.compressorClassName , "compressor.class" );
    assertEquals( columnBinary.columnName , "test" );
    assertEquals( columnBinary.columnType , ColumnType.UNKNOWN );
    assertEquals( columnBinary.rowCount , 100 );
    assertEquals( columnBinary.rawDataSize , 1024 );
    assertEquals( Arrays.equals( columnBinary.binary , new byte[100] ) , true );
    assertEquals( columnBinary.binaryStart , 10 );
    assertEquals( columnBinary.binaryLength , 90 );
    assertEquals( columnBinary.columnBinaryList , null );

    assertTrue( 0 < columnBinary.size() );
    assertTrue( 0 < columnBinary.getMetaSize() );
  }


}
