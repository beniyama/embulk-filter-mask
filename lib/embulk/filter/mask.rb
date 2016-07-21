Embulk::JavaPlugin.register_filter(
  "mask", "org.embulk.filter.mask.MaskFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
