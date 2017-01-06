package org.embulk.filter.mask;

import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.*;
import org.apache.commons.lang3.StringUtils;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.embulk.filter.mask.MaskFilterPlugin.*;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.ArrayList;
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

                if (Types.JSON.equals(inputColumn.getType())) {
                    Value inputJson = (Value) inputValue;
                    DocumentContext context = parseContext.parse(inputJson.toJson());
                    List<Map<String, String>> paths = maskColumn.getPaths().or(new ArrayList<Map<String, String>>());

                    for (Map<String, String> path : paths) {
                        String key = path.get("key");
                        String pattern = path.containsKey("pattern") ? path.get("pattern") : "all";
                        int maskLength = path.containsKey("length") ? Integer.parseInt(path.get("length")) : 0;
                        Object element = context.read(key);
                        if (!key.equals("$") && element != null) {
                            String maskedValue = mask(element, pattern, maskLength);
                            String maskedJson = context.set(key, new TextNode(maskedValue).asText()).jsonString();
                            builder.setJson(inputColumn, jsonParser.parse(maskedJson));
                        }
                    }
                } else {
                    String pattern = maskColumn.getPattern().get();
                    int maskLength = maskColumn.getLength().or(0);
                    String maskedString = mask(inputValue, pattern, maskLength);
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

    private String mask(Object value, String pattern, Integer length) {
        String maskedValue;
        String nakedValue = value.toString();
        if (pattern.equals("email")) {
            if (length > 0) {
                String maskPattern = StringUtils.repeat("*", length) + "@$1";
                maskedValue = nakedValue.replaceFirst("^.+?@(.+)$", maskPattern);
            } else {
                maskedValue = nakedValue.replaceAll(".(?=[^@]*@)", "*");
            }
        } else if (pattern.equals("all")) {
            if (length > 0) {
                maskedValue = StringUtils.repeat("*", length);
            } else {
                maskedValue = nakedValue.replaceAll(".", "*");
            }
        } else {
            maskedValue = nakedValue;
        }
        return maskedValue;
    }
}
