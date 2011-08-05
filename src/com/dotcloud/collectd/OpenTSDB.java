package com.dotcloud.collectd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.opentsdb.core.TSDB;

import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdInitInterface;
import org.collectd.api.CollectdWriteInterface;
import org.collectd.api.DataSource;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;
import org.collectd.api.ValueList;
import org.hbase.async.HBaseClient;

import com.stumbleupon.async.Callback;

public class OpenTSDB implements CollectdWriteInterface, CollectdInitInterface,
        CollectdConfigInterface {

    // Stateless class to log error though collectd
    private final class ErrorCallback implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
            Collectd.logError(arg.getMessage());
            return arg;
        }
    }

    public static final String PLUGIN_NAME = "OpenTSDB";

    // only one instance needed since the class is stateless.
    private final Callback<Exception, Exception> err = new ErrorCallback();
    private TSDB tsdb = null;
    private String quorum = "localhost";
    private String timeseries_table = "tsdb", uniqueids_table = "tsdb-uid";
    private final Map<String, String> tags = new HashMap<String, String>(
            net.opentsdb.core.Const.MAX_NUM_TAGS);

    public OpenTSDB() {
        Collectd.logDebug("OpenTSDB Collectd plugin instancied");

        Collectd.registerConfig(PLUGIN_NAME, this);
        Collectd.registerInit(PLUGIN_NAME, this);
        Collectd.registerWrite(PLUGIN_NAME, this);
    }

    @Override
    public int config(OConfigItem ci) {
        List<OConfigItem> children;

        children = ci.getChildren();
        String key;
        List<OConfigValue> values;

        for (OConfigItem child : children) {
            key = child.getKey();
            if (key.equalsIgnoreCase("quorum")) {
                values = child.getValues();
                if (values.size() != 1) {
                    Collectd.logError(PLUGIN_NAME + ": " + key
                            + "configuration option needs the zookeeper host");
                    return (1);
                } else {
                    this.quorum = values.get(0).toString();
                }
            }

            else if (key.equalsIgnoreCase("timeseries")) {
                values = child.getValues();
                if (values.size() != 1) {
                    Collectd.logError(PLUGIN_NAME
                            + ": "
                            + key
                            + "configuration option needs the timeseries table name");
                    return (1);
                } else {
                    this.timeseries_table = values.get(0).toString();
                }
            } else if (key.equalsIgnoreCase("uidtable")) {
                values = child.getValues();
                if (values.size() != 1) {
                    Collectd.logError(PLUGIN_NAME + ": " + key
                            + "configuration option needs the uid table name");
                    return (1);
                } else {
                    this.uniqueids_table = values.get(0).toString();
                }
            }

            else {
                Collectd.logError(PLUGIN_NAME + ": Unknown config option: "
                        + key);
            }
        }

        return (0);
    }

    @Override
    public int init() {
        final HBaseClient client = new HBaseClient(this.quorum);
        client.setFlushInterval((short) 1000);

        try {
            client.ensureTableExists(this.timeseries_table)
                    .joinUninterruptibly();
            client.ensureTableExists(this.uniqueids_table)
                    .joinUninterruptibly();
        } catch (Exception e) {
            Collectd.logError(e.getMessage());
            return 1;
        }

        this.tsdb = new TSDB(client, this.timeseries_table,
                this.uniqueids_table);

        return 0;
    }

    @Override
    public int write(ValueList vl) {
        List<DataSource> ds = vl.getDataSet().getDataSources();
        List<Number> values = vl.getValues();
        int size = values.size();

        final String plugin = vl.getPlugin();
        final String type = vl.getType();

        if (plugin == null || plugin.isEmpty()) {
            Collectd.logError("plugin value is needed");
            return -1;
        }

        if (type == null || type.isEmpty()) {
            Collectd.logError("type value is needed");
            return -1;
        }

        final String pluginInstance = vl.getPluginInstance();
        final String typeInstance = vl.getTypeInstance();

        tags.clear();

        if (pluginInstance != null && !pluginInstance.isEmpty()) {
            tags.put("plugin_instance", pluginInstance);
        }

        if (typeInstance != null && !typeInstance.isEmpty()) {
            tags.put("type_instance", typeInstance);
        }

        tags.put("host", vl.getHost());

        final String metric = plugin + "." + type;
        final long time = vl.getTime() / 1000;

        for (int i = 0; i < size; i++) {

            final String pointName = ds.get(i).getName();
            if (!pointName.equals("value")) {
                tags.put("value_name", pointName);
            }

            final Number val = values.get(i);
            final String val_s = val.toString();

            if (val_s.indexOf('.') == -1)
                this.tsdb.addPoint(metric, time, val.intValue(), tags)
                        .addErrback(err);
            else
                this.tsdb.addPoint(metric, time, val.floatValue(), tags)
                        .addErrback(err);

        }

        return 0;
    }
}
