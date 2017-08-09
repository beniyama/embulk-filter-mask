package org.embulk.filter.mask;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaskFilterPlugin implements FilterPlugin {
    private final Logger logger = Exec.getLogger(MaskFilterPlugin.class);

    public interface PluginTask extends Task {
        @Config("columns")
        List<MaskColumn> getColumns();

    }

    public interface MaskColumn extends Task {
        @Config("name")
        String getName();

        @Config("type")
        @ConfigDefault("\"all\"")
        Optional<String> getType();

        @Config("pattern")
        @ConfigDefault("\"all\"")
        Optional<String> getPattern();

        @Config("length")
        @ConfigDefault("null")
        Optional<Integer> getLength();

        @Config("start")
        @ConfigDefault("null")
        Optional<Integer> getStart();

        @Config("end")
        @ConfigDefault("null")
        Optional<Integer> getEnd();

        @Config("paths")
        @ConfigDefault("null")
        Optional<List<Map<String, String>>> getPaths();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
                            FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = buildOutputSchema(task, inputSchema);
        control.run(task.dump(), outputSchema);
    }


    private Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        Map<String, MaskColumn> maskColumnMap = getMaskColumnMap(task);
        int i = 0;
        for (Column inputColumn : inputSchema.getColumns()) {
            String name = inputColumn.getName();
            Type type = (maskColumnMap.containsKey(name) && inputColumn.getType() != Types.JSON) ? Types.STRING : inputColumn.getType();
            Column outputColumn = new Column(i++, inputColumn.getName(), type);
            builder.add(outputColumn);
        }

        Schema outputSchema = new Schema(builder.build());
        return outputSchema;
    }

    public static Map<String, MaskColumn> getMaskColumnMap(PluginTask task) {
        Map<String, MaskColumn> maskColumnMap = new HashMap<>();
        for (MaskColumn maskColumn : task.getColumns()) {
            maskColumnMap.put(maskColumn.getName(), maskColumn);
        }
        return maskColumnMap;
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        return new MaskPageOutput(taskSource, inputSchema, outputSchema, output);
    }
}
