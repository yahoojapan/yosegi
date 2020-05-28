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

package jp.co.yahoo.yosegi.util.replacement;

public class PrefixAndSuffix {

  private final IPrefixAppender prefixAppender;
  private final ISuffixAppender suffixAppender;

  /**
   * Initialize by setting prefix and suffix.
   */
  public PrefixAndSuffix( final String prefix , final String suffix , final String delimiter ) {
    if ( delimiter == null ) {
      throw new IllegalArgumentException( "Null delimiters are not allowed." );
    }
    if ( prefix == null ) {
      prefixAppender = new DummyPrefixAppender();
    } else {
      prefixAppender = new PrefixAppender( prefix , delimiter );
    }

    if ( suffix == null ) {
      suffixAppender = new DummySuffixAppender();
    } else {
      suffixAppender = new SuffixAppender( suffix , delimiter );
    }
  }

  public String append( final String str ) {
    return suffixAppender.append( prefixAppender.append( str ) );
  }

  public interface IPrefixAppender {

    String append( final String str );

  }

  public class PrefixAppender implements IPrefixAppender {

    private final String prefixAndDelimiter;

    public PrefixAppender( final String prefix , final String delimiter ) {
      prefixAndDelimiter = String.format( "%s%s" , prefix , delimiter );
    }

    @Override
    public String append( final String str ) {
      return String.format( "%s%s" , prefixAndDelimiter , str );
    }
  }

  public class DummyPrefixAppender implements IPrefixAppender {

    @Override
    public String append( final String str ) {
      return str;
    }
  }

  public interface ISuffixAppender {

    String append( final String str );

  }

  public class SuffixAppender implements ISuffixAppender {

    private final String suffixAndDelimiter;

    public SuffixAppender( final String suffix , final String delimiter ) {
      suffixAndDelimiter = String.format( "%s%s" , delimiter , suffix );
    }

    @Override
    public String append( final String str ) {
      return String.format( "%s%s" , str , suffixAndDelimiter );
    }
  }

  public class DummySuffixAppender implements ISuffixAppender {

    @Override
    public String append( final String str ) {
      return str;
    }
  }

}
