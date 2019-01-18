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

import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.message.parser.IParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ColumnBinaryMakerCustomConfigNode {

  private final String currentColumnName;
  private final ColumnBinaryMakerConfig currentConfig;
  private final Map<String,ColumnBinaryMakerCustomConfigNode> childConfigNode =
      new HashMap<String,ColumnBinaryMakerCustomConfigNode>();

  public ColumnBinaryMakerCustomConfigNode(
      final String currentColumnName ,
      final ColumnBinaryMakerConfig currentConfig ) throws IOException {
    this.currentColumnName = currentColumnName;
    this.currentConfig = currentConfig;
  }

  /**
   * Read setup from JSON and initialize.
   */
  public ColumnBinaryMakerCustomConfigNode(
      final ColumnBinaryMakerConfig commonConfig , final IParser jsonParser ) throws IOException {
    currentColumnName = jsonParser.get( "column_name" ).getString();
    currentConfig = new ColumnBinaryMakerConfig( commonConfig );
    for ( String paramName : jsonParser.getAllKey() ) {
      String value = jsonParser.get( paramName ).getString();
      if ( "child_column".equals( paramName ) ) {
        continue;
      } else if ( "union_maker_class".equals( paramName ) ) {
        currentConfig.unionMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "array_maker_class".equals( paramName ) ) {
        currentConfig.arrayMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "spread_maker_class".equals( paramName ) ) {
        currentConfig.spreadMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "boolean_maker_class".equals( paramName ) ) {
        currentConfig.booleanMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "byte_maker_class".equals( paramName ) ) {
        currentConfig.byteMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "bytes_maker_class".equals( paramName ) ) {
        currentConfig.bytesMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "double_maker_class".equals( paramName ) ) {
        currentConfig.doubleMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "float_maker_class".equals( paramName ) ) {
        currentConfig.floatMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "integer_maker_class".equals( paramName ) ) {
        currentConfig.integerMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "long_maker_class".equals( paramName ) ) {
        currentConfig.longMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "short_maker_class".equals( paramName ) ) {
        currentConfig.shortMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "string_maker_class".equals( paramName ) ) {
        currentConfig.stringMakerClass = FindColumnBinaryMaker.get( value );
      } else if ( "compressor_class".equals( paramName ) ) {
        currentConfig.compressorClass = FindCompressor.get( value );
      } else {
        currentConfig.param.set( paramName , value );
      }
    }

    IParser childArrayParser = jsonParser.getParser( "child_column" );
    for ( int i = 0 ; i < childArrayParser.size() ; i++ ) {
      IParser childParser = childArrayParser.getParser( i );
      ColumnBinaryMakerCustomConfigNode childNodeConfig =
          new ColumnBinaryMakerCustomConfigNode( commonConfig , childParser );
      childConfigNode.put( childNodeConfig.getColumnName() , childNodeConfig );
    }
  }

  public void addChildConfigNode(
      final String columnName , final ColumnBinaryMakerCustomConfigNode child ) {
    childConfigNode.put( columnName , child );
  }

  public ColumnBinaryMakerConfig getCurrentConfig() {
    return currentConfig;
  }

  public ColumnBinaryMakerCustomConfigNode getChildConfigNode(
      final String columnName ) {
    return childConfigNode.get( columnName );
  }

  public String getColumnName() {
    return currentColumnName;
  }


}
