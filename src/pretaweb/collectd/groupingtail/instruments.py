import re
from datetime import datetime
import time
import logging
import logging.handlers
from collections import defaultdict

logger = logging.getLogger("GROUPINGTAIL")

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
        logger.info("groupingtail.instruments.read regex_group %s groups %s\n" % (self.regex_group, self.groups))
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

    def reset(self):
        super(GaugeInt, self).reset()
        self.data = defaultdict(list)
        self.groups = {}

    def read(self):
        logger.info("groupingtail.instruments.gaugeint.read before super\n")
        data_list = super(GaugeInt, self).read()
        logger.info("groupingtail.instruments.gaugeint.read values %s\n", data_list)

        samples = []
        for groupname, values_list in data_list:
            for value in values_list:
                samples.append((groupname, value))

        self.reset()
        logger.info("groupingtail.instruments.gaugeint.read samples %s data %s\n", samples, self.data.items())
        return samples

    def normalise(self):
        # Create newdata with only the members of self.groups and wrap around large integers
        newdata = defaultdict(list)
        logger.info("groupingtail.instruments.gaugeint.normalise group.keys %s\n" % (self.groups.keys()))
        for groupname in self.groups.keys():
            sample_list = list()
            for sample in self.data[groupname]:
                 sample_list.append(self.value_cast(sample))
            newdata[groupname] = sample_list
            logger.info("groupingtail.instruments.gaugeint.normalise keys %s data %s normalized %s\n" % (
            self.groups.keys(), self.data[groupname], newdata[groupname]))
        self.data = newdata

    def append_data(self, groupname, line, mo):
        if self.regex_group:
            value = self.value_cast(mo.groupdict().get(self.regex_group))
        else:
            logger.info("groupingtail.instruments.gaugeint.append_data no regex_group line %s\n", line)
            value = self.value_cast(mo.groups()[0])
        logger.info("groupingtail.instruments.gaugeint.append_data regex_group %s data %s value %s line %s\n",
                    self.regex_group, self.data.items(), value, line)
        self.data[groupname].append(value)


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
            value = self.value_cast(mo.groupdict().get(self.regex_group))
        else:
            value = self.value_cast(mo.groups()[0])
        current = self.data.get(groupname, None)
        if value > current or current is None:
            self.data[groupname] = value


class DeriveCounter(CounterSum):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(DeriveCounter, self).__init__(*args, **kwargs)
        self.last_read = None

    def reset(self):
        super(DeriveCounter, self).reset()
        self.last_read = None

    def read(self):
        elapsed = 0
        now = time.time()
        if self.last_read is not None:
            elapsed = now - self.last_read
        self.last_read = now
        if elapsed <= 0:
            elapsed = 1.0
        logger.info("groupingtail.instruments.DeriveCounter.read last %s now %s elapsed %f\n", str(self.last_read), str(now), elapsed)
        data_list = super(DeriveCounter, self).read()

        samples = []
        for groupname, value in data_list:
            samples.append((groupname, value/elapsed))

        logger.info("groupingtail.instruments.DeriveCounter.read samples %s data %s\n", samples, self.data.items())
        return samples
