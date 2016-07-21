package org.embulk.filter.mask;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.MockFormatterPlugin;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestMaskFilterPlugin {
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private ConfigSource getConfigFromYaml(String yaml) {
        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yaml);
    }

    @Test
    public void testThrowExceptionAtMissingColumnsField() {
        String configYaml = "type: mask";
        ConfigSource config = getConfigFromYaml(configYaml);

        exception.expect(ConfigException.class);
        exception.expectMessage("Field 'columns' is required but not set");
        config.loadConfig(MockFormatterPlugin.PluginTask.class);
    }
}
