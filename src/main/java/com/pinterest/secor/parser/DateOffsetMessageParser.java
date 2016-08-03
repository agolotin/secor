/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

/**
 * DateOffsetMessageParser extracts timestamp field (specified by 'message.timestamp.name') 
 *  and the date pattern (specified by 'message.timestamp.input.pattern') and also adds the offset
 *  using the same method that the offset mesage parser uses
 * 
 * @see http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html
 * 
 * @author Artem Golotin (artem.golotin@gmail.com)
 * 
 */
public class DateOffsetMessageParser extends MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(DateOffsetMessageParser.class);
    protected static final String defaultDate = "dt=1970-01-01&offset=000000"; // TODO figure out how many zeros actually go here....
    protected static final String defaultFormatter = "yyyy-MM-dd";
    protected SimpleDateFormat outputFormatter = new SimpleDateFormat(defaultFormatter);
    protected Object inputPattern;
    protected SimpleDateFormat inputFormatter;
    
    public DateOffsetMessageParser(SecorConfig config) {
        super(config);
        TimeZone timeZone = config.getTimeZone();
        inputPattern = mConfig.getMessageTimestampInputPattern();
        inputFormatter = new SimpleDateFormat(inputPattern.toString());
        inputFormatter.setTimeZone(timeZone);
        outputFormatter.setTimeZone(timeZone);
    }

    @Override
    public String[] extractPartitions(Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        String result[] = { defaultDate };

        if (jsonObject != null) {
            Object fieldValue = getJsonFieldValue(jsonObject);
            if (fieldValue != null && inputPattern != null) {
                try {
                	// Get the date
                    Date dateFormat = inputFormatter.parse(fieldValue.toString());
                    result[0] = "dt=" + outputFormatter.format(dateFormat);
                    // Get the offset
					long offset = message.getOffset();
					long offsetsPerPartition = mConfig.getOffsetsPerPartition();
					long partition = (offset / offsetsPerPartition) * offsetsPerPartition;
					result[0] += "&offset=" + partition;
						
                    return result;
                } catch (Exception e) {

                    LOG.warn("Impossible to convert date = {} for the input pattern = {} . Using date default = {}",
                            fieldValue.toString(), inputPattern.toString(), result[0]);
                }
            }
        }
        return result;
    }
}