package org.embulk.filter.mask;

import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.embulk.filter.mask.MaskFilterPlugin.*;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaskPageOutput implements PageOutput {
    private final MaskFilterPlugin.PluginTask task;
    private final Map<String, Column> outputColumnMap;
    private final List<Column> inputColumns;
    private final Map<String, MaskColumn> maskColumnMap;
    private final PageReader reader;
    private final PageBuilder builder;
    private final ParseContext parseContext;
    private final JsonParser jsonParser;
    private final Logger logger = Exec.getLogger(MaskPageOutput.class);

    public MaskPageOutput(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        this.task = taskSource.loadTask(MaskFilterPlugin.PluginTask.class);
        this.inputColumns = inputSchema.getColumns();
        this.maskColumnMap = MaskFilterPlugin.getMaskColumnMap(this.task);
        this.reader = new PageReader(inputSchema);
        this.builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
        this.outputColumnMap = new HashMap<>();
        for (Column column : outputSchema.getColumns()) {
            this.outputColumnMap.put(column.getName(), column);
        }
        this.parseContext = initializeParseContext();
        this.jsonParser = new JsonParser();
    }

    private ParseContext initializeParseContext() {
        Configuration conf = Configuration.defaultConfiguration();
        conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        conf = conf.addOptions(Option.SUPPRESS_EXCEPTIONS);
        return JsonPath.using(conf);
    }

    @Override
    public void add(Page page) {
        reader.setPage(page);
        while (reader.nextRecord()) {
            setValue();
            builder.addRecord();
        }
    }

    private void setValue() {
        for (Column inputColumn : inputColumns) {
            if (reader.isNull(inputColumn)) {
                builder.setNull(inputColumn);
                continue;
            }

            Object inputValue;
            if (Types.STRING.equals(inputColumn.getType())) {
                final String value = reader.getString(inputColumn);
                inputValue = value;
                builder.setString(inputColumn, value);
            } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                final boolean value = reader.getBoolean(inputColumn);
                inputValue = value;
                builder.setBoolean(inputColumn, value);
            } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                final double value = reader.getDouble(inputColumn);
                inputValue = value;
                builder.setDouble(inputColumn, value);
            } else if (Types.LONG.equals(inputColumn.getType())) {
                final long value = reader.getLong(inputColumn);
                inputValue = value;
                builder.setLong(inputColumn, value);
            } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                final Timestamp value = reader.getTimestamp(inputColumn);
                inputValue = value;
                builder.setTimestamp(inputColumn, value);
            } else if (Types.JSON.equals(inputColumn.getType())) {
                final Value value = reader.getJson(inputColumn);
                inputValue = value;
                builder.setJson(inputColumn, value);
            } else {
                throw new DataException("Unexpected type:" + inputColumn.getType());
            }

            if (maskColumnMap.containsKey(inputColumn.getName())) {
                MaskColumn maskColumn = maskColumnMap.get(inputColumn.getName());
                String targetValue = inputValue.toString();
                String pattern = maskColumn.getPattern().get();

                if (Types.JSON.equals(inputColumn.getType())) {
                    String path = maskColumn.getPath().get();
                    String element = parseContext.parse(targetValue).read(path);
                    String maskedValue = mask(element, pattern);
                    String maskedJson = parseContext.parse(targetValue).set(path, new TextNode(maskedValue).asText()).jsonString();
                    builder.setJson(inputColumn, jsonParser.parse(maskedJson));
                } else {
                    String maskedString = mask(targetValue, pattern);
                    builder.setString(inputColumn, maskedString);
                }
            }
        }
    }

    @Override
    public void finish() {
        builder.finish();
    }

    @Override
    public void close() {
        builder.close();
    }

    private String mask(String value, String pattern) {
        String maskedValue;
        if (pattern.equals("email")) {
            maskedValue = value.replaceAll(".(?=[^@]*@)", "*");
        } else if (pattern.equals("all")) {
            maskedValue = value.replaceAll(".", "*");
        } else {
            maskedValue = value;
        }
        return maskedValue;
    }
}
