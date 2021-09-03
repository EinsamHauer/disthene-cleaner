package net.iponweb.disthene.cleaner;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

/**
 * Hello world!
 */
public class DistheneCleaner {
    private static Logger logger;

    private static final int DEFAULT_THREADS = 32;
//    private static final String DEFAULT_ROLLUP_STRING = "60:5356800,900:62208000";
    private static final long DEFAULT_THRESHOLD = 2678400;

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("l", "log-location", true, "log file location");
        options.addOption("ll", "log-level", true, "log level (i.e.: DEBUG, INFO, ERROR, etc)");
        options.addOption("r", "rollups", true, "rollups, format like 900:62208000");
        options.addOption("t", "threads", true, "number of threads");
        options.addOption("c", "cassandra", true, "Cassandra contact point");
        options.addOption("e", "elasticsearch", true, "Elasticsearch contact point");
        options.addOption("th", "threshold", true, "Threshold");
        options.addOption("ex", "exclude", true, "Exclude paths, comma separated list of wildcards");
        options.addOption("tn", "tenant", true, "Tenant");
        options.addOption("n", "noop", false, "Noop mode");

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine commandLine = parser.parse(options, args);

            String logLocation = commandLine.hasOption("l") ? commandLine.getOptionValue("l") : null;
            String logLevel = commandLine.hasOption("ll") ? commandLine.getOptionValue("ll") : "INFO";
            configureLog(logLocation, logLevel);

            final DistheneCleanerParameters parameters = new DistheneCleanerParameters();

            if (!commandLine.hasOption("tn")) {
                logger.error("Tenant is not specified");
                System.exit(4);
            }

            if (!commandLine.hasOption("c")) {
                logger.error("Cassandra contact point is not specified");
                System.exit(5);
            }

            if (!commandLine.hasOption("e")) {
                logger.error("Elasticsearch contact point is not specified");
                System.exit(6);
            }

            parameters.setThreads(DEFAULT_THREADS);
            if (commandLine.hasOption("t")) {
                try {
                    parameters.setThreads(Integer.parseInt(commandLine.getOptionValue("t")));
                } catch (Exception ignored) {

                }
            }

            parameters.setTenant(commandLine.getOptionValue("tn"));
            parameters.setCassandraContactPoint(commandLine.getOptionValue("c"));
            parameters.setElasticSearchContactPoint(commandLine.getOptionValue("e"));

            parameters.setThreshold(DEFAULT_THRESHOLD);
            if (commandLine.hasOption("th")) {
                try {
                    parameters.setThreshold(Long.parseLong(commandLine.getOptionValue("th")));
                } catch (Exception ignored) {
                }
            }

            if (commandLine.hasOption("ex")) {
                String[] split = commandLine.getOptionValue("ex").split(",");
                for (String exclusion : split) {
                    parameters.addExclusion(exclusion);
                }
            }

            if (commandLine.hasOption("n")) {
                parameters.setNoop(true);
            }

            logger.info("Running with the following parameters: " + parameters);
            new Cleaner(parameters).clean();
            logger.info("All done");
            System.exit(0);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Disthene-cleaner", options);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Unexpected error: ", e);
            System.exit(100);
        }
    }

    private static void configureLog(String location, String level) {
        Level logLevel = Level.toLevel(level, Level.INFO);

        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

        RootLoggerComponentBuilder rootLogger = builder.newRootLogger(logLevel);

        // console
        LayoutComponentBuilder layout = builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%p %d{dd.MM.yyyy HH:mm:ss,SSS} [%t] %c %x - %m%n");

        AppenderComponentBuilder console = builder.newAppender("stdout", "Console").add(layout);
        builder.add(console);
        rootLogger.add(builder.newAppenderRef("stdout"));

        // file
        if (location != null) {
            AppenderComponentBuilder rollingFile = builder.newAppender("rolling", "RollingFile");
            rollingFile.addAttribute("fileName", location);
            rollingFile.addAttribute("filePattern", location + "-%d{MM-dd-yy}.log.gz");

            @SuppressWarnings("rawtypes")
            ComponentBuilder triggeringPolicies = builder.newComponent("Policies")
                    .addComponent(builder.newComponent("TimeBasedTriggeringPolicy")
                            .addAttribute("interval", "1"));
            rollingFile.addComponent(triggeringPolicies);

            builder.add(rollingFile);

            rootLogger.add(builder.newAppenderRef("rolling"));
        }

        builder.add(rootLogger);

        Configurator.initialize(builder.build());

        logger = LogManager.getLogger(DistheneCleaner.class);
    }

}
