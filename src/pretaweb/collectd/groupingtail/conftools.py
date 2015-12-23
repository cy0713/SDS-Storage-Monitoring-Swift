import logging
from groupingtail import GroupingTail
from instruments import NUM32, CounterInc, CounterSum, GaugeInt, DeriveCounter, GaugeThroughput, GaugeTotalThroughput

_getConfFirstValue_NOVAL = object()

logger = logging.getLogger("GROUPINGTAIL")


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
    # Regex to do the matching
    regex = getConfFirstValue(conf, 'Regex')
    return CounterInc(regex)


def configure_countersumint(conf):
    # Regex to do the matching
    regex = getConfFirstValue(conf, "Regex")
    # Regex group name, or None that acts as first group in regex
    groupname = getConfFirstValue(conf, "GroupName", None)
    # Function to perform casting to data extracted from field groupname of regex
    value_cast = (lambda x: int(x) % NUM32)
    return CounterSum(regex, value_cast=value_cast, groupname=groupname)


def configure_gaugeint(conf):
    # Regex to do the matching
    regex = getConfFirstValue(conf, "Regex")
    # Regex group name, or None that acts as first group in regex
    groupname = getConfFirstValue(conf, "GroupName", None)
    # Function to perform casting to data extracted from field groupname of regex
    value_cast = (lambda x: int(x) % NUM32)
    return GaugeInt(regex, value_cast=value_cast, groupname=groupname)


def configure_derivecounter(conf):
    # Regex to do the matching
    regex = getConfFirstValue(conf, "Regex")
    # Regex group name, or None that acts as first group in regex
    groupname = getConfFirstValue(conf, "GroupName", None)
    # Function to perform casting to data extracted from field groupname of regex
    value_cast = (lambda x: int(x) % NUM32)
    return DeriveCounter(regex, value_cast=value_cast, groupname=groupname)


def configure_gaugethroughput(conf):
    # Regex to do the matching
    regex = getConfFirstValue(conf, "Regex")
    # Regex group name, or None that acts as first group in regex
    groupname = getConfFirstValue(conf, "GroupName", None)
    # Regex group name, or None that acts as first group in regex
    grouptime = getConfFirstValue(conf, "GroupTime", None)
    return GaugeThroughput(regex, groupname=groupname, grouptime=grouptime)


def configure_gaugetotalthroughput(conf):
    # Regex to do the matching
    regex = getConfFirstValue(conf, "Regex")
    groupone = getConfFirstValue(conf, "GroupOne", None)
    groupother = getConfFirstValue(conf, "GroupOther", None)
    grouptime = getConfFirstValue(conf, "GroupTime", None)
    return GaugeTotalThroughput(regex, groupone=groupone, groupother=groupother, grouptime=grouptime)


# Dict with configurations of each instrument available
INSTRUMENTS = {
    "CounterInc": configure_counterinc,
    "CounterSumInt": configure_countersumint,
    "GaugeInt": configure_gaugeint,
    "GaugeThroughput": configure_gaugethroughput,
    "GaugeTotalThroughput": configure_gaugetotalthroughput,
    "DeriveCounter": configure_derivecounter
}


def read_config(conf):
    files = []
    logger.info("groupingtail.conftools.read_config loop\n")
    # Read all <file>*</file> blocks in config file
    for f in getConfChildren(conf, "File"):
        instance_name = getConfFirstValue(f, 'Instance')
        filepath = f.values[0]

        # Regex to do grouping
        groupby = getConfFirstValue(f, 'GroupBy')
        # Regex group name, or None that acts as first group in regex
        groupbygroup = getConfFirstValue(f, 'GroupName', None)
        # Maximum number of groups
        maxgroups = int(getConfFirstValue(f, 'MaxGroups', 64))

        logger.info("groupingtail.conftools.read_config filepath %s groupby %s groupbygroup %s\n" % (
            filepath, groupby, groupbygroup))
        gt = GroupingTail(filepath, groupby, groupbygroup)

        # List with files to check
        files.append(dict(
            instance_name=instance_name,
            grouping_tail=gt
        ))

        # For every matching in the file
        for m in getConfChildren(f, 'Match'):
            # Maching name
            minstance_name = getConfFirstValue(m, "Instance")
            # Maching type
            valuetype = getConfFirstValue(m, "Type")

            # dstype determines the instrument used
            dstype = getConfFirstValue(m, "DSType")

            # read and create instrument
            instrument = INSTRUMENTS[dstype](m)
            logger.info("groupingtail.conftools.read_config instance_name %s valuetype %s instrument %s\n" % (
                minstance_name, valuetype, instrument))
            # Add matching to groupingtail
            gt.add_match(minstance_name, valuetype, instrument)
    return files
