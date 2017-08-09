package org.embulk.filter.mask;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.TestPageBuilderReader.*;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.value.Value;

import java.util.List;


import static org.embulk.filter.mask.MaskFilterPlugin.PluginTask;
import static org.embulk.filter.mask.MaskFilterPlugin.Control;
import static org.embulk.spi.type.Types.*;
import static org.junit.Assert.assertEquals;
import static org.msgpack.value.ValueFactory.*;

public class TestMaskFilterPlugin {
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private static Value s(String value) {
        return newString(value);
    }

    private static Value i(int value) {
        return newInteger(value);
    }

    private static Value f(double value) {
        return newFloat(value);
    }

    private static Value b(boolean value) {
        return newBoolean(value);
    }

    private ConfigSource getConfigFromYaml(String yaml) {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    private String getMaskedCharacters(Object value) {
        String maskedValue = "";
        for (int i = 0; i < value.toString().length(); i++) {
            maskedValue += "*";
        }
        return maskedValue;
    }

    private String getMaskedEmail(String email) {
        String maskedValue = "";
        for (int i = 0; i < email.length(); i++) {
            if (email.charAt(i) == '@') {
                maskedValue += email.substring(i);
                break;
            }
            maskedValue += "*";
        }
        return maskedValue;
    }

    @Test
    public void testThrowExceptionAtMissingColumnsField() {
        String configYaml = "type: mask";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'columns' is required but not set");
        config.loadConfig(PluginTask.class);
    }

    @Test
    public void testOnlyMaskTargetColumns() {
        String configYaml = "" +
                "type: mask\n" +
                "columns:\n" +
                "  - { name: _c0}\n" +
                "  - { name: _c2}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("_c0", STRING)
                .add("_c1", STRING)
                .add("_c2", STRING)
                .add("_c3", STRING)
                .build();

        final MaskFilterPlugin maskFilterPlugin = new MaskFilterPlugin();
        maskFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                final String c0ColumnValue = "_c0_THIS_MUST_BE_MASKED";
                final String c1ColumnValue = "_c1_THIS_MUST_NOT_BE_MASKED";
                final String c2ColumnValue = "_c2_THIS_MUST_BE_MASKED_ALSO";
                final String c3ColumnValue = "_c3_THIS_MUST_NOT_BE_MASKED_ALSO";

                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = maskFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            c0ColumnValue,
                            c1ColumnValue,
                            c2ColumnValue,
                            c3ColumnValue
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);

                assertEquals(1, records.size());
                Object[] record = records.get(0);

                assertEquals(4, record.length);
                assertEquals(getMaskedCharacters(c0ColumnValue), record[0]);
                assertEquals(c1ColumnValue, record[1]);
                assertEquals(getMaskedCharacters(c2ColumnValue), record[2]);
                assertEquals(c3ColumnValue, record[3]);
            }
        });
    }

    @Test
    public void testPassVarietyOfTypes() {
        String configYaml = "" +
                "type: mask\n" +
                "columns:\n" +
                "  - { name: _dummy}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("_c0", STRING)
                .add("_c1", BOOLEAN)
                .add("_c2", DOUBLE)
                .add("_c3", LONG)
                .add("_c4", TIMESTAMP)
                .add("_c5", JSON)
                .build();

        final MaskFilterPlugin maskFilterPlugin = new MaskFilterPlugin();
        maskFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                final String c0ColumnValue = "_c0_STRING";
                final Boolean c1ColumnValue = false;
                final Double c2ColumnValue = 12345.6789;
                final Long c3ColumnValue = Long.MAX_VALUE;
                final Timestamp c4ColumnValue = Timestamp.ofEpochSecond(4);
                final Value c5ColumnValue = newMapBuilder().put(s("_c5"), s("_v5")).build();

                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = maskFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            c0ColumnValue,
                            c1ColumnValue,
                            c2ColumnValue,
                            c3ColumnValue,
                            c4ColumnValue,
                            c5ColumnValue
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);

                assertEquals(1, records.size());
                Object[] record = records.get(0);

                assertEquals(6, record.length);
                assertEquals(c0ColumnValue, record[0]);
                assertEquals(c1ColumnValue, record[1]);
                assertEquals(c2ColumnValue, record[2]);
                assertEquals(c3ColumnValue, record[3]);
                assertEquals(c4ColumnValue, record[4]);
                assertEquals(c5ColumnValue, record[5]);
            }
        });
    }

    @Test
    public void testMaskVarietyOfTypes() {
        String configYaml = "" +
                "type: mask\n" +
                "columns:\n" +
                "  - { name: _c0}\n" +
                "  - { name: _c1}\n" +
                "  - { name: _c2}\n" +
                "  - { name: _c3}\n" +
                "  - { name: _c4}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("_c0", STRING)
                .add("_c1", BOOLEAN)
                .add("_c2", DOUBLE)
                .add("_c3", LONG)
                .add("_c4", TIMESTAMP)
                .build();

        final MaskFilterPlugin maskFilterPlugin = new MaskFilterPlugin();
        maskFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                final String c0ColumnValue = "_c0_STRING";
                final Boolean c1ColumnValue = false;
                final Double c2ColumnValue = 12345.6789;
                final Long c3ColumnValue = Long.MAX_VALUE;
                final Timestamp c4ColumnValue = Timestamp.ofEpochSecond(4);

                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = maskFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            c0ColumnValue,
                            c1ColumnValue,
                            c2ColumnValue,
                            c3ColumnValue,
                            c4ColumnValue
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);

                assertEquals(1, records.size());
                Object[] record = records.get(0);

                assertEquals(5, record.length);
                assertEquals(getMaskedCharacters(c0ColumnValue), record[0]);
                assertEquals(getMaskedCharacters(c1ColumnValue), record[1]);
                assertEquals(getMaskedCharacters(c2ColumnValue), record[2]);
                assertEquals(getMaskedCharacters(c3ColumnValue), record[3]);
                assertEquals(getMaskedCharacters(c4ColumnValue), record[4]);
            }
        });
    }

    @Test
    public void testMaskJson() {
        String configYaml = "" +
                "type: mask\n" +
                "columns:\n" +
                "  - { name: _c0}\n" +
                "  - { name: _c1, paths: [{key: $.root.key1}]}\n" +
                "  - { name: _c2, paths: [{key: $.root.key3, length: 2}, {key: $.root.key4, type: all}]}\n" +
                "  - { name: _c3, paths: [{key: $.root.key1}, {key: $.root.key3.key7, type: email, length: 3}]}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("_c0", JSON)
                .add("_c1", JSON)
                .add("_c2", JSON)
                .add("_c3", JSON)
                .build();

        final MaskFilterPlugin maskFilterPlugin = new MaskFilterPlugin();
        maskFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                final Value jsonValue = newMapBuilder().put(
                        s("root"),
                        newMap(
                                s("key1"), s("value1"),
                                s("key2"), i(2),
                                s("key3"), newMap(
                                        s("key5"), s("value5"),
                                        s("key6"), newArray(i(0), i(1), i(2), i(3), i(4)),
                                        s("key7"), s("testme@example.com")
                                ),
                                s("key4"), newArray(i(0), i(1), i(2), i(3), i(4))
                        )
                ).build();

                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = maskFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            jsonValue,
                            jsonValue,
                            jsonValue,
                            jsonValue
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);

                assertEquals(1, records.size());
                Object[] record = records.get(0);

                assertEquals(4, record.length);
                assertEquals("{\"root\":{\"key1\":\"value1\",\"key2\":2,\"key3\":{\"key5\":\"value5\",\"key6\":[0,1,2,3,4],\"key7\":\"testme@example.com\"},\"key4\":[0,1,2,3,4]}}", record[0].toString());
                assertEquals("{\"root\":{\"key1\":\"******\",\"key2\":2,\"key3\":{\"key5\":\"value5\",\"key6\":[0,1,2,3,4],\"key7\":\"testme@example.com\"},\"key4\":[0,1,2,3,4]}}", record[1].toString());
                assertEquals("{\"root\":{\"key1\":\"value1\",\"key2\":2,\"key3\":\"**\",\"key4\":\"***********\"}}", record[2].toString());
                assertEquals("{\"root\":{\"key1\":\"******\",\"key2\":2,\"key3\":{\"key5\":\"value5\",\"key6\":[0,1,2,3,4],\"key7\":\"***@example.com\"},\"key4\":[0,1,2,3,4]}}", record[3].toString());
            }
        });
    }

    @Test
    public void testMaskEmail() {
        String configYaml = "" +
                "type: mask\n" +
                "columns:\n" +
                "  - { name: _c0, type: email}\n" +
                "  - { name: _c1, type: email}\n" +
                "  - { name: _c2, type: all}\n" +
                "  - { name: _c3}\n";

        ConfigSource config = getConfigFromYaml(configYaml);

        final Schema inputSchema = Schema.builder()
                .add("_c0", STRING)
                .add("_c1", STRING)
                .add("_c2", STRING)
                .add("_c3", STRING)
                .add("_c4", STRING)
                .build();

        final MaskFilterPlugin maskFilterPlugin = new MaskFilterPlugin();
        maskFilterPlugin.transaction(config, inputSchema, new Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema) {
                final String email1 = "dummy_test-me.1234@dummy-mail1.com";
                final String email2 = "!#$%&'*+-/=?^_`.{|}~@dummy-mail2.com";

                MockPageOutput mockPageOutput = new MockPageOutput();
                try (PageOutput pageOutput = maskFilterPlugin.open(taskSource, inputSchema, outputSchema, mockPageOutput)) {
                    for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema,
                            email1,
                            email2,
                            email1,
                            email1,
                            email1
                    )) {
                        pageOutput.add(page);
                    }
                    pageOutput.finish();
                }
                List<Object[]> records = Pages.toObjects(outputSchema, mockPageOutput.pages);

                assertEquals(1, records.size());
                Object[] record = records.get(0);

                assertEquals(5, record.length);
                assertEquals(getMaskedEmail(email1), record[0]);
                assertEquals(getMaskedEmail(email2), record[1]);
                assertEquals(getMaskedCharacters(email1), record[2]);
                assertEquals(getMaskedCharacters(email1), record[3]);
                assertEquals(email1, record[4]);
            }
        });
    }
}
