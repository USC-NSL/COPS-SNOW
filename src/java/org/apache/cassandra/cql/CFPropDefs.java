/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.cql;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CFPropDefs {
    private static Logger logger = LoggerFactory.getLogger(CFPropDefs.class);

    public static final String KW_COMPARATOR = "comparator";
    public static final String KW_COMMENT = "comment";
    public static final String KW_READREPAIRCHANCE = "read_repair_chance";
    public static final String KW_GCGRACESECONDS = "gc_grace_seconds";
    public static final String KW_DEFAULTVALIDATION = "default_validation";
    public static final String KW_MINCOMPACTIONTHRESHOLD = "min_compaction_threshold";
    public static final String KW_MAXCOMPACTIONTHRESHOLD = "max_compaction_threshold";
    public static final String KW_REPLICATEONWRITE = "replicate_on_write";

    public static final String KW_COMPACTION_STRATEGY_CLASS = "compaction_strategy_class";

    // Maps CQL short names to the respective Cassandra comparator/validator class names
    public static final Map<String, String> comparators = new HashMap<String, String>();
    public static final Set<String> keywords = new HashSet<String>();
    public static final Set<String> obsoleteKeywords = new HashSet<String>();
    public static final Set<String> allowedKeywords = new HashSet<String>();

    public static final String COMPACTION_OPTIONS_PREFIX = "compaction_strategy_options";
    public static final String COMPRESSION_PARAMETERS_PREFIX = "compression_parameters";

    static
    {
        comparators.put("ascii", "AsciiType");
        comparators.put("bigint", "LongType");
        comparators.put("blob", "BytesType");
        comparators.put("boolean", "BooleanType");
        comparators.put("counter", "CounterColumnType");
        comparators.put("decimal", "DecimalType");
        comparators.put("double", "DoubleType");
        comparators.put("float", "FloatType");
        comparators.put("int", "Int32Type");
        comparators.put("text", "UTF8Type");
        comparators.put("timestamp", "DateType");
        comparators.put("uuid", "UUIDType");
        comparators.put("varchar", "UTF8Type");
        comparators.put("varint", "IntegerType");

        keywords.add(KW_COMPARATOR);
        keywords.add(KW_COMMENT);
        keywords.add(KW_READREPAIRCHANCE);
        keywords.add(KW_GCGRACESECONDS);
        keywords.add(KW_DEFAULTVALIDATION);
        keywords.add(KW_MINCOMPACTIONTHRESHOLD);
        keywords.add(KW_MAXCOMPACTIONTHRESHOLD);
        keywords.add(KW_REPLICATEONWRITE);
        keywords.add(KW_COMPACTION_STRATEGY_CLASS);

        obsoleteKeywords.add("row_cache_size");
        obsoleteKeywords.add("key_cache_size");
        obsoleteKeywords.add("row_cache_save_period_in_seconds");
        obsoleteKeywords.add("key_cache_save_period_in_seconds");
        obsoleteKeywords.add("memtable_throughput_in_mb");
        obsoleteKeywords.add("memtable_operations_in_millions");
        obsoleteKeywords.add("memtable_flush_after_mins");
        obsoleteKeywords.add("row_cache_provider");

        allowedKeywords.addAll(keywords);
        allowedKeywords.addAll(obsoleteKeywords);
    }

    public final Map<String, String> properties = new HashMap<String, String>();
    public final Map<String, String> compactionStrategyOptions = new HashMap<String, String>();
    public final Map<String, String> compressionParameters = new HashMap<String, String>();

    public void validate() throws InvalidRequestException
    {
        // we need to remove parent:key = value pairs from the main properties
        Set<String> propsToRemove = new HashSet<String>();

        // check if we have compaction/compression options
        for (String property : properties.keySet())
        {
            if (!property.contains(":"))
                continue;

            String key = property.split(":")[1];
            String val = properties.get(property);

            if (property.startsWith(COMPACTION_OPTIONS_PREFIX))
            {
                compactionStrategyOptions.put(key, val);
                propsToRemove.add(property);
            }

            if (property.startsWith(COMPRESSION_PARAMETERS_PREFIX))
            {
                compressionParameters.put(key, val);
                propsToRemove.add(property);
            }
        }

        for (String property : propsToRemove)
            properties.remove(property);
        // Catch the case where someone passed a kwarg that is not recognized.
        for (String bogus : Sets.difference(properties.keySet(), allowedKeywords))
            throw new InvalidRequestException(bogus + " is not a valid keyword argument for CREATE COLUMNFAMILY");
        for (String obsolete : Sets.intersection(properties.keySet(), obsoleteKeywords))
            logger.warn("Ignoring obsolete property {}", obsolete);

        // Validate min/max compaction thresholds
        Integer minCompaction = getPropertyInt(KW_MINCOMPACTIONTHRESHOLD, null);
        Integer maxCompaction = getPropertyInt(KW_MAXCOMPACTIONTHRESHOLD, null);

        if ((minCompaction != null) && (maxCompaction != null))     // Both min and max are set
        {
            if ((minCompaction > maxCompaction) && (maxCompaction != 0))
                throw new InvalidRequestException(String.format("%s cannot be larger than %s",
                        KW_MINCOMPACTIONTHRESHOLD,
                        KW_MAXCOMPACTIONTHRESHOLD));
        }
        else if (minCompaction != null)     // Only the min threshold is set
        {
            if (minCompaction > CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD)
                throw new InvalidRequestException(String.format("%s cannot be larger than %s, (default %s)",
                        KW_MINCOMPACTIONTHRESHOLD,
                        KW_MAXCOMPACTIONTHRESHOLD,
                        CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD));
        }
        else if (maxCompaction != null)     // Only the max threshold is set
        {
            if ((maxCompaction < CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD) && (maxCompaction != 0))
                throw new InvalidRequestException(String.format("%s cannot be smaller than %s, (default %s)",
                        KW_MAXCOMPACTIONTHRESHOLD,
                        KW_MINCOMPACTIONTHRESHOLD,
                        CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD));
        }
    }

    /** Map a keyword to the corresponding value */
    public void addProperty(String name, String value)
    {
        properties.put(name, value);
    }

    public Boolean hasProperty(String name)
    {
        return properties.containsKey(name);
    }

    /* If not comparator/validator is not specified, default to text (BytesType is the wrong default for CQL
     * since it uses hex terms).  If the value specified is not found in the comparators map, assume the user
     * knows what they are doing (a custom comparator/validator for example), and pass it on as-is.
     */

    public AbstractType<?> getComparator() throws ConfigurationException
    {
        return TypeParser.parse((comparators.get(getPropertyString(KW_COMPARATOR, "text")) != null)
                                  ? comparators.get(getPropertyString(KW_COMPARATOR, "text"))
                                  : getPropertyString(KW_COMPARATOR, "text"));
    }

    public AbstractType<?> getValidator() throws ConfigurationException
    {
        return TypeParser.parse((comparators.get(getPropertyString(KW_DEFAULTVALIDATION, "text")) != null)
                                  ? comparators.get(getPropertyString(KW_DEFAULTVALIDATION, "text"))
                                  : getPropertyString(KW_DEFAULTVALIDATION, "text"));
    }

    public String getProperty(String name)
    {
        return properties.get(name);
    }

    public String getPropertyString(String key, String defaultValue)
    {
        String value = properties.get(key);
        return value != null ? value : defaultValue;
    }

    // Return a property value, typed as a Boolean
    public Boolean getPropertyBoolean(String key, Boolean defaultValue) throws InvalidRequestException
    {
        String value = properties.get(key);
        return (value == null) ? defaultValue : value.toLowerCase().matches("(1|true|yes)");
    }

    // Return a property value, typed as a Double
    public Double getPropertyDouble(String key, Double defaultValue) throws InvalidRequestException
    {
        Double result;
        String value = properties.get(key);

        if (value == null)
            result = defaultValue;
        else
        {
            try
            {
                result = Double.parseDouble(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidRequestException(String.format("%s not valid for \"%s\"", value, key));
            }
        }
        return result;
    }

    // Return a property value, typed as an Integer
    public Integer getPropertyInt(String key, Integer defaultValue) throws InvalidRequestException
    {
        Integer result;
        String value = properties.get(key);

        if (value == null)
            result = defaultValue;
        else
        {
            try
            {
                result = Integer.parseInt(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidRequestException(String.format("%s not valid for \"%s\"", value, key));
            }
        }
        return result;
    }

    public String toString()
    {
        return String.format("CFPropDefs(%s, compaction: %s, compression: %s)",
                             properties.toString(),
                             compactionStrategyOptions.toString(),
                             compressionParameters.toString());
    }
}
