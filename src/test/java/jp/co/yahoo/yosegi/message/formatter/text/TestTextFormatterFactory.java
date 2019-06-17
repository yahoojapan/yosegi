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

package jp.co.yahoo.yosegi.message.formatter.text;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.BooleanField;
import jp.co.yahoo.yosegi.message.design.BytesField;
import jp.co.yahoo.yosegi.message.design.DoubleField;
import jp.co.yahoo.yosegi.message.design.FloatField;
import jp.co.yahoo.yosegi.message.design.IField;
import jp.co.yahoo.yosegi.message.design.IntegerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.MapContainerField;
import jp.co.yahoo.yosegi.message.design.NullField;
import jp.co.yahoo.yosegi.message.design.Properties;
import jp.co.yahoo.yosegi.message.design.ShortField;
import jp.co.yahoo.yosegi.message.design.StringField;
import jp.co.yahoo.yosegi.message.design.StructContainerField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;

public class TestTextFormatterFactory {

  @Test
  public void getFromArrayField() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ITextFormatter formatter = TextFormatterFactory.get( new ArrayContainerField(
        "field" , new StringField( "child" ) , option ) );
    assertTrue( ( formatter instanceof TextArrayFormatter ) );
  }

  @Test
  public void getFromStructField() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    ITextFormatter formatter = TextFormatterFactory.get( new StructContainerField( "field" , option ) );
    assertTrue( ( formatter instanceof TextStructFormatter ) );
  }

  @Test
  public void getFromMapField() throws IOException {
    Properties option = new Properties();
    option.set( "delimiter" , "0x2c" );
    option.set( "field_delimiter" , "0x3d" );
    ITextFormatter formatter = TextFormatterFactory.get( new MapContainerField(
        "field" , new StringField( "child" ) , option ) );
    assertTrue( ( formatter instanceof TextMapFormatter ) );
  }

  @Test
  public void getFromBooleanField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new BooleanField( "field" ) );
    assertTrue( ( formatter instanceof TextBooleanFormatter ) );
  }

  @Test
  public void getFromBytesField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new BytesField( "field" ) );
    assertTrue( ( formatter instanceof TextBytesFormatter ) );
  }

  @Test
  public void getFromDoubleField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new DoubleField( "field" ) );
    assertTrue( ( formatter instanceof TextDoubleFormatter ) );
  }

  @Test
  public void getFromFloatField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new FloatField( "field" ) );
    assertTrue( ( formatter instanceof TextFloatFormatter ) );
  }

  @Test
  public void getFromIntegerField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new IntegerField( "field" ) );
    assertTrue( ( formatter instanceof TextIntegerFormatter ) );
  }

  @Test
  public void getFromLongField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new LongField( "field" ) );
    assertTrue( ( formatter instanceof TextLongFormatter ) );
  }

  @Test
  public void getFromShortField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new ShortField( "field" ) );
    assertTrue( ( formatter instanceof TextShortFormatter ) );
  }

  @Test
  public void getFromStringField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new StringField( "field" ) );
    assertTrue( ( formatter instanceof TextStringFormatter ) );
  }

  @Test
  public void getFromNullField() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( new NullField( "field" ) );
    assertTrue( ( formatter instanceof TextNullFormatter ) );
  }

  @Test
  public void getFromNull() throws IOException {
    ITextFormatter formatter = TextFormatterFactory.get( null );
    assertTrue( ( formatter instanceof TextNullFormatter ) );
  }

}
