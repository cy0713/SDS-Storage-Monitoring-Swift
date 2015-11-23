import re
from datetime import datetime
import logging
import logging.handlers

logger = logging.getLogger("CAMAMILLA")

NUM32 = 2 ** 32


class Instrument(object):
    def __init__(self, regex, maxgroups=64, value_cast=float, groupname=None):
        logger.info("groupingtail.instruments.constructor groupname %s regex %s\n" % (groupname, regex))

        self.test = re.compile(regex)
        self.maxgroups = maxgroups
        self.value_cast = value_cast
        self.regex_group = groupname

        self.data = None
        self.groups = None
        self.reset()

    def reset(self):
        # Empty bucket and start again
        self.data = {}
        self.groups = {}

    def touch_group(self, groupname):
        self.groups[groupname] = datetime.now()

    def trim_groups(self):
        # Trim groups which haven't been touched
        items = self.groups.items()
        items.sort()
        self.groups = dict(items[:self.maxgroups])

    def normalise(self):
        # Create newdata with only the members of self.groups and wrap around large integers
        newdata = {}
        logger.info("groupingtail.instruments.normalise group.keys %s\n" % (self.groups.keys()))
        for groupname in self.groups.keys():
            newdata[groupname] = self.value_cast(self.data[groupname])
            logger.info("groupingtail.instruments.normalise keys %s data %s normalized %s\n" % (
            self.groups.keys(), self.data[groupname], newdata[groupname]))
        self.data = newdata

    def read(self):
        # Return the current results of the bucket
        self.trim_groups()
        logger.info("groupingtail.instruments.read trim data %s\n" % (self.data.items()))
        self.normalise()
        logger.info("groupingtail.instruments.read normalise data %s\n" % (self.data.items()))
        return self.data.items()

    def append_data(self, groupname, line, mo):
        # do actual data analysis
        logger.info("groupingtail.instruments.append_data ERROR mo %s groupname %s line %s\n" % (mo, groupname, line))
        raise NotImplementedError()

    def write(self, groupname, line):
        # analise log line
        mo = self.test.match(line)
        logger.info("groupingtail.instruments.write mo %s groupname %s line %s\n" % (mo, groupname, line))
        if mo is not None:
            try:
                logger.info(
                    "groupingtail.instruments.write.append_data mo %s groupname %s line %s\n" % (mo, groupname, line))
                self.append_data(groupname, line, mo)
            except ValueError:
                logger.info("groupingtail.instruments.write ERROR groupname %s line %s\n" % (groupname, line))
                pass
            except Exception as e:
                # the instrument failed.
                logger.info(
                    "groupingtail.instruments.write instrument failed groupname %s regex_group %s Exception %s\n" % (
                    groupname, self.regex_group, e))
                self.reset()
            else:
                logger.info("groupingtail.instruments.write touch group groupname %s line %s\n" % (groupname, line))
                self.touch_group(groupname)


class GaugeInt(Instrument):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(GaugeInt, self).__init__(*args, **kwargs)

    def read(self):
        values = super(GaugeInt, self).read()
        logger.info("groupingtail.instruments.gaugeint.read values %s\n", values)
        self.reset()
        return values

    def append_data(self, groupname, line, mo):
        logger.info("groupingtail.instruments.gaugeint.append_data line %s\n", line)
        if self.regex_group:
            logger.info("groupingtail.instruments.gaugeint.append_data regex_group %s mo.groupdict %s\n",
                        self.regex_group, mo.groupdict)
            value = self.value_cast(mo.groupdict.get(self.regex_group))
        else:
            logger.info("groupingtail.instruments.gaugeint.append_data no regex_group line %s\n", line)
            value = self.value_cast(mo.groups()[0])
        logger.info("groupingtail.instruments.gaugeint.append_data regex_group %s data %s value %s line %s\n",
                    self.regex_group, self.data.items(), value, line)
        self.data[groupname] = value


class CounterInc(Instrument):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(CounterInc, self).__init__(*args, **kwargs)

    def append_data(self, groupname, line, mo):
        logger.info("groupingtail.instruments.counterinc.append_data data %s line %s\n", self.data.get(groupname, 0),
                    line)
        self.data[groupname] = self.data.get(groupname, 0) + 1


class CounterSum(Instrument):
    def append_data(self, groupname, line, mo):
        minimum = self.value_cast(0)
        if self.regex_group:
            groups = mo.groupdict()
            value = self.value_cast(groups.get(self.regex_group))
        else:
            value = self.value_cast(mo.groups()[0])
        self.data[groupname] = self.data.get(groupname, minimum) + value


class Max(GaugeInt):
    def append_data(self, groupname, line, mo):
        if self.regex_group:
            value = self.value_cast(mo.groupdict.get(self.regex_group))
        else:
            value = self.value_cast(mo.groups()[0])
        current = self.data.get(groupname, None)
        if value > current or current is None:
            self.data[groupname] = value
