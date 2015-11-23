import collectd
import logging
import logging.handlers
from conftools import read_config

log_file_trace_path = "/home/swift/pretaweb.collectd.groupingtail/src/pretaweb/collectd/custom_collectd_groupingtail.log"

logger = logging.getLogger("CAMAMILLA")
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(log_file_trace_path, maxBytes=20000000, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s];[%(thread)d];%(asctime)s;%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


#
# Configuration
#
files = None


def configure(conf):
    global files
    files = read_config(conf)
    logger.info("groupingtail.plugin.configure %d\n" % (len(files)))


#
# Getting mesurements
#
def update():
    logger.info("groupingtail.plugin.update loop over %d\n" % len(files))
    for f in files:
        logger.info("groupingtail.plugin.updating %s\n" % f)
        f["grouping_tail"].update()


def read():
    # this might be good in another thread
    logger.info("groupingtail.plugin.read.update %d\n" % len(files))
    update()
    logger.info("groupingtail.plugin.read %d\n" % len(files))
    for f in files:
        instance_name = f["instance_name"]
        gt = f["grouping_tail"]
        logger.info("groupingtail.plugin.pattern %s\n" % gt.groupmatch.pattern)
        for metric_name, value_type, value in gt.read_metrics():
            logger.info("groupingtail.plugin.read dispatch metric %s %s %s\n" % (metric_name, value_type, value))
            v = collectd.Values(
                plugin='groupingtail',
                plugin_instance="%s.%s" % (instance_name, metric_name),
                type=value_type,
                values=(value,)
            )
            v.dispatch()


# Register functions with collectd
logger.info("groupingtail.plugin.register before\n")
collectd.register_config(configure)
collectd.register_read(read)
logger.info("groupingtail.plugin.register after\n")
