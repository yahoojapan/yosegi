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

package jp.co.yahoo.yosegi.config;

import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerConfig;
import jp.co.yahoo.yosegi.binary.ColumnBinaryMakerCustomConfigNode;
import jp.co.yahoo.yosegi.binary.optimizer.FindOptimizerFactory;
import jp.co.yahoo.yosegi.binary.optimizer.IOptimizerFactory;
import jp.co.yahoo.yosegi.block.ReadColumnUtil;
import jp.co.yahoo.yosegi.compressor.FindCompressor;
import jp.co.yahoo.yosegi.compressor.ICompressor;
import jp.co.yahoo.yosegi.encryptor.EncryptionKey;
import jp.co.yahoo.yosegi.encryptor.EncryptionSettingNode;
import jp.co.yahoo.yosegi.encryptor.FindEncryptorFactory;
import jp.co.yahoo.yosegi.encryptor.IEncryptor;
import jp.co.yahoo.yosegi.encryptor.IEncryptorFactory;
import jp.co.yahoo.yosegi.keystore.KeyStore;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.util.List;

public final class YosegiConfiguration {

  public static final String PROP_BLOCK_COMPRESS_CLASS =
      "block.maker.compress.class";
  public static final String PROP_COLUMN_MAKER_DEFAULT_COMPRESS_CLASS = 
      "spread.column.maker.default.compress.class";

  public static final String PROP_COLUMN_MAKER_SETTING =
      "spread.column.maker.setting";
  public static final String PROP_COLUMN_MAKER_USE_AUTO_OPTIMIZER =
      "spread.column.maker.use.auto.optimizer";
  public static final String PROP_COLUMN_MAKER_OPTIMIZER_CLASS =
      "spread.column.maker.use.auto.optimizer.factory.class";

  public static final String PROP_COMPRESS_OPTIMIZE_ALLOWED_RATIO =
      "compress.optimize.allowed.ratio";

  public static final String PROP_KEY_STORE_SETTING = 
      "keystore.setting";

  public static final String PROP_ENCRYPT_FACTORY_CLASS =
      "encrypt.factory.class";
  public static final String PROP_BLOCK_ENCRYPT_SETTING =
      "block.encrypt.setting";
  public static final String PROP_COLUMN_ENCRYPT_SETTING =
      "column.encrypt.setting";
  public static final String PROP_ENCRYPT_AAD_PREFIX =
      "encrypt.aad.prefix";
  public static final String PROP_ENCRYPT_AAD_PREFIX_DISABLE_WRITE =
      "encrypt.aad.prefix.disable.write";

  public static final String PROP_READ_COLUMN_NAME =
      "spread.reader.read.column.names";

  /**
   * Whether to use the optimizer.
   */
  public static boolean useBinaryAutoOptimizer( final Configuration config ) throws IOException {
    if ( config.containsKey( PROP_COLUMN_MAKER_USE_AUTO_OPTIMIZER ) ) {
      return config.get( PROP_COLUMN_MAKER_USE_AUTO_OPTIMIZER , "true" ).equals( "true" );
    } else {
      return true;
    }
  }

  /**
   * Whether to use column maker setting.
   */
  public static boolean useUserColumnMakerSetting( final Configuration config ) throws IOException {
    return config.containsKey( PROP_COLUMN_MAKER_SETTING );
  }

  /**
   * Get user column maker setting.
   */
  public static ColumnBinaryMakerCustomConfigNode getUserColumnMakerSetting(
      final Configuration config ,
      final ColumnBinaryMakerConfig defaultConfig ) throws IOException {
    if ( useUserColumnMakerSetting( config ) ) {
      JacksonMessageReader jsonReader = new JacksonMessageReader();
      IParser jsonParser = jsonReader.create( config.get( PROP_COLUMN_MAKER_SETTING ) );
      return new ColumnBinaryMakerCustomConfigNode( defaultConfig , jsonParser );
    } else {
      return new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    }
  }

  /**
   * Get auto optimizer factory.
   */
  public static IOptimizerFactory getBinaryAutoOptimizerFactory(
      final Configuration config ) throws IOException {
    return FindOptimizerFactory.get( config.get(
        PROP_COLUMN_MAKER_OPTIMIZER_CLASS ,
        "jp.co.yahoo.yosegi.binary.optimizer.DefaultOptimizerFactory" ) , config );
  }

  /**
   * Get block compressor.
   */
  public static ICompressor getBlockCompressor( final Configuration config ) throws IOException {
    return FindCompressor.get( config.get(
        PROP_BLOCK_COMPRESS_CLASS ,
        "jp.co.yahoo.yosegi.compressor.DefaultCompressor" ) );
  }

  /**
   * Get default compressor for column maker.
   */
  public static ICompressor getDefaultCompressorForColumnMaker(
      final Configuration config ) throws IOException {
    return FindCompressor.get( config.get(
        PROP_COLUMN_MAKER_DEFAULT_COMPRESS_CLASS ,
        "jp.co.yahoo.yosegi.compressor.GzipCompressor" ) );
  }

  /**
   * Get the ratio that allows compression optimization.
   */
  public static double getCompressOptimizeAllowedRatio( final Configuration config ) {
    double allowedRatio = config.getDouble( "compress.optimize.allowed.ratio" , 1.15d );
    if ( 0 < Double.valueOf( allowedRatio ).compareTo( 0d ) ) {
      return allowedRatio;
    } else {
      return 1.15d;
    }
  }

  /**
   * Get key store from json.
   */
  public static KeyStore getKeyStore( final Configuration config ) throws IOException {
    if ( config.containsKey( PROP_KEY_STORE_SETTING ) ) {
      return KeyStore.createFromJson( config.get( PROP_KEY_STORE_SETTING ) );
    } else {
      return new KeyStore();
    }
  }

  /**
   * Get encryption keys.
   */
  public static EncryptionKey[] getEncryptionKeys(
      final Configuration config ) throws IOException {
    if ( config.containsKey( "keystore.setting" ) ) {
      return KeyStore.createKeysFromJson( config.get( "keystore.setting" ) );
    } else {
      return new EncryptionKey[0];
    }
  }

  /**
   * Get encryptor factory.
   */
  public static IEncryptorFactory getEncryptorFactory(
      final Configuration config ) throws IOException {
    return FindEncryptorFactory.get( config.get(
        PROP_ENCRYPT_FACTORY_CLASS ,
        "jp.co.yahoo.yosegi.encryptor.AesGcmEncryptorFactory" ) );
  }

  /**
   * Get encryption setting node about block.
   */
  public static EncryptionSettingNode getBlockEncryptionSettingNode(
      final Configuration config , final KeyStore keyStore ) throws IOException {
    if ( config.containsKey( PROP_BLOCK_ENCRYPT_SETTING ) ) {
      return EncryptionSettingNode.createFromJson(
          config.get( PROP_BLOCK_ENCRYPT_SETTING ) , keyStore.getKeyNameSet() );
    } else {
      return new EncryptionSettingNode( "root" , null );
    }
  }

  /**
   * Get encryption setting node about column.
   */
  public static EncryptionSettingNode getColumnEncryptionSettingNode(
      final Configuration config , final KeyStore keyStore ) throws IOException {
    if ( config.containsKey( PROP_COLUMN_ENCRYPT_SETTING ) ) {
      return EncryptionSettingNode.createFromJson(
          config.get( PROP_COLUMN_ENCRYPT_SETTING ) , keyStore.getKeyNameSet() );
    } else {
      return new EncryptionSettingNode( "root" , null );
    }
  }

  /**
   * Get aad prefix.
   */
  public static byte[] getAadPrefix( final Configuration config ) throws IOException {
    if ( config.containsKey( PROP_ENCRYPT_AAD_PREFIX ) ) {
      byte[] aadPrefix = config.get( PROP_ENCRYPT_AAD_PREFIX ).getBytes();
      if ( 128 <= aadPrefix.length ) {
        throw new IOException( "aad prefix has 127 byte size limit." );
      }
      if ( aadPrefix.length == 0 ) {
        return new byte[0];
      }
      return aadPrefix;
    } else {
      return new byte[0];
    }
  }

  public static boolean isWritingAadPrefixDisabled(
      final Configuration config ) throws IOException {
    return config.get( PROP_ENCRYPT_AAD_PREFIX_DISABLE_WRITE , "false" ).equals( "true" );
  }

  public static List<String[]> getReadColumnName(
      final Configuration config ) throws IOException {
    return ReadColumnUtil.readColumnSetting( config.get( PROP_READ_COLUMN_NAME ) );
  }

}
