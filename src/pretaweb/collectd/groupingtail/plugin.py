import collectd
import logging
import logging.handlers
from conftools import read_config

log_file_trace_path = "/var/log/collectd_groupingtail_plugin.log"

logger = logging.getLogger("GROUPINGTAIL")
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(log_file_trace_path, maxBytes=10000000, backupCount=1)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s];[%(thread)d];%(asctime)s;%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


# Entry point of plugin groupingtail for collectd daemon

# List of dictionaries of files and matching configurations
files = None


# Collectd register_config implementation
def configure(conf):
    global files
    files = read_config(conf)


#
# Getting mesurements
#
def update():
    # For all matchings in all files update its state
    for f in files:
        f["grouping_tail"].update()


# Collectd register_read implementation
def read():
    # this might be good in another thread
    update()
    for f in files:
        instance_name = f["instance_name"]
        gt = f["grouping_tail"]

        # Extract metrics info from all groupingtail configurations
        for metric_name, value_type, value in gt.read_metrics():
            # Create collectd value
            v = collectd.Values(
                plugin='groupingtail',
                plugin_instance="%s*%s" % (instance_name, metric_name),
                type=value_type,
                values=(value,)
            )

            # Dispatch value to collectd daemon
            v.dispatch()


# Register functions to collectd daemon
collectd.register_config(configure)
collectd.register_read(read)
