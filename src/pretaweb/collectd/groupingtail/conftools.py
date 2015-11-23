import logging
from groupingtail import GroupingTail
from instruments import NUM32, CounterInc, CounterSum, GaugeInt

_getConfFirstValue_NOVAL = object()

logger = logging.getLogger("CAMAMILLA")


def getConfFirstValue(ob, key, default=_getConfFirstValue_NOVAL):
    for o in ob.children:
        if o.key.lower() == key.lower():
            return o.values[0]
    if default == _getConfFirstValue_NOVAL:
        raise KeyError()
    return default


def getConfChildren(ob, key):
    children = []
    for o in ob.children:
        if o.key.lower() == key.lower():
            children.append(o)
    return children


#
# Instruments library and generators
#
def configure_counterinc(conf):
    regex = getConfFirstValue(conf, 'Regex')
    logger.info("groupingtail.conftools.configure_counterinc %s\n" % (regex))
    return CounterInc(regex)


def configure_countersumint(conf):
    regex = getConfFirstValue(conf, "Regex")
    groupname = getConfFirstValue(conf, "GroupName", None)
    value_cast = (lambda x: int(x) % NUM32)
    return CounterSum(regex, value_cast=value_cast, groupname=groupname)


def configure_gaugeint(conf):
    regex = getConfFirstValue(conf, "Regex")
    groupname = getConfFirstValue(conf, "GroupName", None)
    value_cast = (lambda x: int(x) % NUM32)
    return GaugeInt(regex, value_cast=value_cast, groupname=groupname)


INSTRUMENTS = {
    "CounterInc": configure_counterinc,
    "CounterSumInt": configure_countersumint,
    "GaugeInt": configure_gaugeint
}


def read_config(conf):
    files = []
    logger.info("groupingtail.conftools.read_config loop\n")
    for f in getConfChildren(conf, "File"):
        instance_name = getConfFirstValue(f, 'Instance')
        filepath = f.values[0]
        groupby = getConfFirstValue(f, 'GroupBy')
        groupbygroup = getConfFirstValue(f, 'GroupName', None)
        maxgroups = int(getConfFirstValue(f, 'MaxGroups', 64))

        logger.info("groupingtail.conftools.read_config filepath %s groupby %s groupbygroup %s\n" % (
            filepath, groupby, groupbygroup))
        gt = GroupingTail(filepath, groupby, groupbygroup)

        files.append(dict(
            instance_name=instance_name,
            grouping_tail=gt
        ))

        for m in getConfChildren(f, 'Match'):
            minstance_name = getConfFirstValue(m, "Instance")
            valuetype = getConfFirstValue(m, "Type")

            # dstype determins the instrument used
            dstype = getConfFirstValue(m, "DSType")
            instrument = INSTRUMENTS[dstype](m)
            logger.info("groupingtail.conftools.read_config minstance_name %s valuetype %s instrument %s\n" % (
                minstance_name, valuetype, instrument))
            gt.add_match(minstance_name, valuetype, instrument)
    return files
