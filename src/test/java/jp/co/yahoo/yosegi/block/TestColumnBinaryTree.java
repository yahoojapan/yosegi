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
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.UnsafeOptimizeDumpStringColumnBinaryMaker;
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.spread.Spread;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import jp.co.yahoo.yosegi.spread.column.PrimitiveColumn;
import jp.co.yahoo.yosegi.spread.column.IColumn;
import jp.co.yahoo.yosegi.spread.column.filter.*;
import jp.co.yahoo.yosegi.spread.expression.*;
import jp.co.yahoo.yosegi.util.ByteArrayData;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestColumnBinaryTree{

  private List<ColumnBinary> createSingleCaseData() throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column1" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );

    UnsafeOptimizeDumpStringColumnBinaryMaker maker = new UnsafeOptimizeDumpStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    return Arrays.asList(
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column ) ); 
  }

  private List<ColumnBinary> createSimpleCaseData() throws IOException {
    IColumn column = new PrimitiveColumn( ColumnType.STRING , "column1" );
    column.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );

    IColumn column2 = new PrimitiveColumn( ColumnType.STRING , "column2" );
    column2.add( ColumnType.STRING , new StringObj( "a" ) , 0 );
    column2.add( ColumnType.STRING , new StringObj( "ab" ) , 1 );
    column2.add( ColumnType.STRING , new StringObj( "abc" ) , 2 );
    column2.add( ColumnType.STRING , new StringObj( "abcd" ) , 3 );
    column2.add( ColumnType.STRING , new StringObj( "b" ) , 4 );
    column2.add( ColumnType.STRING , new StringObj( "bc" ) , 5 );
    column2.add( ColumnType.STRING , new StringObj( "bcd" ) , 6 );
    column2.add( ColumnType.STRING , new StringObj( "bcde" ) , 7 );
    column2.add( ColumnType.STRING , new StringObj( "c" ) , 8 );
    column2.add( ColumnType.STRING , new StringObj( "cd" ) , 9 );

    UnsafeOptimizeDumpStringColumnBinaryMaker maker = new UnsafeOptimizeDumpStringColumnBinaryMaker();
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );

    return Arrays.asList(
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column ) ,
        maker.toBinary( defaultConfig , null , new CompressResultNode() , column2 ) );

  }

  @Test
  public void T_getColumnBinary_1() throws IOException{
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
    tree.add( new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null ) );

    assertEquals( tree.getColumnBinary( 0 ).columnName , "test"  );
  }

  @Test
  public void T_add_1() throws IOException{
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
    tree.add( new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null ) );

    assertEquals( tree.getColumnBinary( 0 ).columnName , "test"  );
  }

  @Test
  public void T_add_2() throws IOException{
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = new ArrayList<ColumnBinary>();
    childList.add( new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , null ) );
    tree.add( new ColumnBinary( "hoge.class" , "compressor.class" , "test" , ColumnType.UNKNOWN , 100 , 1024 , 100 , -1 , new byte[100] , 10 , 90 , childList ) );

    assertEquals( tree.getColumnBinary( 0 ).columnName , "test"  );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withOnce() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list );
    tree.addChild( list );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withSomeList() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list );
    tree.addChild( list );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withOneColumnAfterTwoColumns() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSingleCaseData();
    tree.addChild( list );
    List<ColumnBinary> list2 = createSimpleCaseData();
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withTwoColumnsAfterOneColumn() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    tree.addChild( list );
    List<ColumnBinary> list2 = createSingleCaseData();
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withNestedSimpleCaseChild() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list );
    tree.addChild( list );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withSomeNestedSimpleCaseChild() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list );
    tree.addChild( list );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withNestedFirstOneColumnAfterTwoColumn() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSingleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    tree.addChild( list );

    List<ColumnBinary> childList2 = createSimpleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withNestedFirstTwoColumnsAfterOneColumn() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    tree.addChild( list );

    List<ColumnBinary> childList2 = createSingleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_metaSizeAfterAppend_equalsMetaBinarySize_withNestedFirstIsNoting() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    tree.addChild( list );

    List<ColumnBinary> childList2 = createSimpleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int metaSizeAfterAppend = tree.metaSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( metaSizeAfterAppend , tree.metaSize() );

    ByteArrayData byteArrayData = new ByteArrayData();
    tree.createMeta( byteArrayData , 0 );
    assertEquals( byteArrayData.getLength() , metaSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withOnce() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list );
    tree.addChild( list );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withSomeList() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list );
    tree.addChild( list );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withOneColumnAfterTwoColumns() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSingleCaseData();
    tree.addChild( list );
    List<ColumnBinary> list2 = createSimpleCaseData();
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withTwoColumnsAfterOneColumn() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    tree.addChild( list );
    List<ColumnBinary> list2 = createSingleCaseData();
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedSimpleCaseChild() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list );
    tree.addChild( list );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withSomeNestedSimpleCaseChild() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    tree.addChild( list );
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list );
    tree.addChild( list );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedFirstOneColumnAfterTwoColumn() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSingleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    tree.addChild( list );

    List<ColumnBinary> childList2 = createSimpleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedFirstTwoColumnsAfterOneColumn() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> childList = createSimpleCaseData();
    List<ColumnBinary> list = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList ) );
    tree.addChild( list );

    List<ColumnBinary> childList2 = createSingleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

  @Test
  public void T_dataSizeAfterAppend_equalsDataBinarySize_withNestedFirstIsNoting() throws IOException {
    ColumnBinaryTree tree = new ColumnBinaryTree();
    List<ColumnBinary> list = createSimpleCaseData();
    tree.addChild( list );

    List<ColumnBinary> childList2 = createSimpleCaseData();
    List<ColumnBinary> list2 = Arrays.asList( DumpSpreadColumnBinaryMaker.createSpreadColumnBinary(
        "parent" , 10 , childList2 ) );
    int dataSizeAfterAppend = tree.dataSizeAfterAppend( list2 );
    tree.addChild( list2 );
    assertEquals( dataSizeAfterAppend , tree.dataSize() );

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    tree.writeData( out );
    byte[] data = out.toByteArray();
    assertEquals( data.length , dataSizeAfterAppend );
  }

}
