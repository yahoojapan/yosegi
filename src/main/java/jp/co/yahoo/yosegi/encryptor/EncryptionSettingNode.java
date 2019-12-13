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

package jp.co.yahoo.yosegi.encryptor;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EncryptionSettingNode {

  private final String columnName;
  private final String keyName;

  private Map<String,EncryptionSettingNode> childNodeMap
      = new HashMap<String,EncryptionSettingNode>();

  /**
   * Initialization.
   */
  public EncryptionSettingNode(
      final String columnName ,
      final String keyName ) {
    this.columnName = columnName;
    this.keyName = keyName;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getKeyName() {
    return keyName;
  }

  public void putChildNode( final EncryptionSettingNode childNode ) {
    childNodeMap.put( childNode.getColumnName() , childNode );
  }

  public boolean isEncryptNode() {
    return keyName != null && ! keyName.isEmpty();
  }

  /**
   * Get child node.
   */
  public EncryptionSettingNode getChildNode( final String childColumnName ) {
    if ( ! childNodeMap.containsKey( childColumnName ) ) {
      childNodeMap.put( childColumnName ,
          new EncryptionSettingNode( childColumnName , keyName ) );
    }
    return childNodeMap.get( childColumnName );
  }

  /**
   * Create a node from json.
   */
  public static EncryptionSettingNode createFromJson(
      final String json , final Set<String> keyNameSet ) throws IOException {
    return createFromParser( new JacksonMessageReader().create( json ) , keyNameSet );
  }

  /**
   * Create a node from IParser.
   */
  public static EncryptionSettingNode createFromParser(
      final IParser parser , final Set<String> keyNameSet ) throws IOException {
    EncryptionSettingNode rootNode = new EncryptionSettingNode( "root" , null );
    setFromParser( rootNode , parser , keyNameSet );
    return rootNode;
  } 

  private static void setFromParser(
      final EncryptionSettingNode parentNode ,
      final IParser parser ,
      final Set<String> keyNameSet ) throws IOException {
    String[] columnNames = parser.getAllKey();
    for ( String columnName : columnNames ) {
      IParser childParser = parser.getParser( columnName );
      String keyName = null;
      if ( childParser.containsKey( "key_name" ) ) {
        keyName = childParser.get( "key_name" ).getString(); 
      }
      if ( keyName != null && ! childParser.containsKey( "key_name" ) ) {
        if ( ! keyNameSet.contains( keyName ) ) {
          throw new IOException(
              String.format( "The specified key name \"%s\" does not exist.", keyName ) );
        }
      }
      EncryptionSettingNode node =
          new EncryptionSettingNode( columnName , keyName );
      if ( childParser.containsKey( "child" ) ) {
        setFromParser( node , childParser.getParser( "child" ) , keyNameSet );
      }
      parentNode.putChildNode( node );
    }
  }

}
