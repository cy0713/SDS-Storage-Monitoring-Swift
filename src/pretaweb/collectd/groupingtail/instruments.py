import re
from datetime import datetime
import time
import logging
import logging.handlers

logger = logging.getLogger("GROUPINGTAIL")

NUM32 = 2 ** 32


# Parser parent class
class Instrument(object):
    def __init__(self, regex, maxgroups=64, value_cast=float, groupname=None):
        logger.info("groupingtail.instruments.constructor groupname %s regex %s\n" % (groupname, regex))

        # Compiled regex
        self.test = re.compile(regex)
        # Maxim number of groups
        self.maxgroups = maxgroups
        # Cast function
        self.value_cast = value_cast
        # Regex groupname
        self.regex_group = groupname

        # Data collected and processed
        self.data = None
        # Groups timestamps
        self.groups = None

        # Reset data and groups structures
        self.reset()

    # Empty bucket and start again
    def reset(self):
        self.data = {}
        self.groups = {}

    # Mark groups tstamp
    def touch_group(self, groupname):
        self.groups[groupname] = datetime.now()

    # Trim groups which haven't been touched
    def trim_groups(self):
        items = self.groups.items()
        items.sort()
        self.groups = dict(items[:self.maxgroups])

    # Create newdata with only the members of self.groups and wrap around large integers
    def normalise(self):
        newdata = {}
        for groupname in self.groups.keys():
            newdata[groupname] = self.value_cast(self.data[groupname])
        self.data = newdata

    # Return the current results of the bucket
    def read(self):
        self.trim_groups()
        self.normalise()
        return self.data.items()

    # Do actual data analysis from line
    def append_data(self, groupname, line, mo):
        raise NotImplementedError()

    # Performs matching over line line and do proper analysis
    def write(self, groupname, line):
        # analise log line
        mo = self.test.match(line)
        logger.info("groupingtail.instruments.instrument.write regex %s line %s\n", self.test.pattern, line)
        # If matching
        if mo is not None:
            try:
                # Perform analysis
                self.append_data(groupname, line, mo)
            except ValueError:
                # Contemplated error
                pass
            except Exception as e:
                # the instrument failed.
                self.reset()
            else:
                # Mark updated group
                self.touch_group(groupname)


class GaugeInt(Instrument):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(GaugeInt, self).__init__(*args, **kwargs)

    def read(self):
        data_list = super(GaugeInt, self).read()

        self.reset()
        return data_list

    def append_data(self, groupname, line, mo):
        if self.regex_group:
            value = self.value_cast(mo.groupdict().get(self.regex_group))
        else:
            value = self.value_cast(mo.groups()[0])

        self.data[groupname] = self.data.get(groupname, 0) + value


class GaugeThroughput(Instrument):
    def __init__(self, *args, **kwargs):
        self.grouptime = kwargs["grouptime"]
        del kwargs["grouptime"]
        self.value_index = 0
        self.elapsed_index = 1
        super(GaugeThroughput, self).__init__(*args, **kwargs)

    def read(self):
        data = super(GaugeThroughput, self).read()

        self.reset()
        return data

    def append_data(self, groupname, line, mo):
        if self.regex_group is not None and self.grouptime is not None:
            value = self.value_cast(mo.groupdict().get(self.regex_group))
            elapsed = self.value_cast(mo.groupdict().get(self.grouptime))

            current = self.data.get(groupname, [0, 0])
            current[self.value_index] += value
            current[self.elapsed_index] += elapsed
            self.data[groupname] = current

    def normalise(self):
        # Create newdata with only the members of self.groups and wrap around large integers
        newdata = {}
        for groupname in self.groups.keys():
            sample_list = self.data[groupname]
            value = sample_list[self.value_index]
            elapsed = sample_list[self.elapsed_index]
            if elapsed > 0.0:
                bw = value/elapsed
            else:
                bw = value
            newdata[groupname] = bw
        self.data = newdata


class GaugeTotalThroughput(GaugeThroughput):
    def __init__(self, *args, **kwargs):
        self.groupone = kwargs["groupone"]
        self.groupother = kwargs["groupother"]
        del kwargs["groupone"]
        del kwargs["groupother"]
        super(GaugeTotalThroughput, self).__init__(*args, **kwargs)

    def append_data(self, groupname, line, mo):
        elapsed = self.value_cast(mo.groupdict().get(self.grouptime))
        value = None
        try:
            value = self.value_cast(mo.groupdict().get(self.groupone))
        except:
            try:
                value = self.value_cast(mo.groupdict().get(self.groupother))
            except:
                pass

        if value is not None:
            current = self.data.get(groupname, [0, 0])
            current[self.value_index] += value
            current[self.elapsed_index] += elapsed
            self.data[groupname] = current


class CounterInc(Instrument):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(CounterInc, self).__init__(*args, **kwargs)

    def append_data(self, groupname, line, mo):
        self.data[groupname] = self.data.get(groupname, 0) + 1

    def read(self):
        data_list = super(CounterInc, self).read()
        self.reset()
        return data_list


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
        data_list = super(DeriveCounter, self).read()

        samples = []
        for groupname, value in data_list:
            samples.append((groupname, value/elapsed))

        return samples
