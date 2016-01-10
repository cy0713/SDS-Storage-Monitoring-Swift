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
        # If matching
        if mo is not None:
            try:
                # Perform analysis
                self.append_data(groupname, line, mo)
            except ValueError:
                # Contemplated error
                pass
            except Exception as e:
                # The instrument has failed.
                self.reset()
            else:
                # Mark updated group
                self.touch_group(groupname)


# Incremental Instrument from 0 with value of line matching
class GaugeInt(Instrument):
    def __init__(self, *args, **kwargs):
        # Lambda function to cast into a int
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(GaugeInt, self).__init__(*args, **kwargs)

    def read(self):
        # Return the current results of the bucket
        data_list = super(GaugeInt, self).read()

        # Empty bucket and start again
        self.reset()
        return data_list

    def append_data(self, groupname, line, mo):
        # Do actual data analysis from line
        if self.regex_group:
            # With groupname
            value = self.value_cast(mo.groupdict().get(self.regex_group))
        else:
            # First group
            value = self.value_cast(mo.groups()[0])

        # Update stored data
        self.data[groupname] = self.data.get(groupname, 0) + value


# Throughput Instrument for one measurement
class GaugeThroughput(Instrument):
    def __init__(self, *args, **kwargs):
        self.grouptime = kwargs["grouptime"]
        del kwargs["grouptime"]
        # List indexs
        self.value_index = 0
        self.elapsed_index = 1
        super(GaugeThroughput, self).__init__(*args, **kwargs)

    def read(self):
        # Return the current results of the bucket
        data = super(GaugeThroughput, self).read()

        # Empty bucket and start again
        self.reset()
        return data

    def append_data(self, groupname, line, mo):
        # Do actual data analysis from line
        if self.regex_group is not None and self.grouptime is not None:
            # Extract value from line
            value = self.value_cast(mo.groupdict().get(self.regex_group))
            # Extract value from line
            elapsed = self.value_cast(mo.groupdict().get(self.grouptime))

            # Get current throughput info list
            current = self.data.get(groupname, [0, 0])
            # Increment bytes transferred
            current[self.value_index] += value
            # Increment time elapsed
            current[self.elapsed_index] += elapsed

            # Update stored data
            self.data[groupname] = current

    def normalise(self):
        # Create newdata with only the members of self.groups and wrap around large integers
        newdata = {}
        # For all groupingnames
        for groupname in self.groups.keys():
            # Get throughput info list
            sample_list = self.data[groupname]
            # Get total bytes transferred
            value = sample_list[self.value_index]
            # Get total time elapsed
            elapsed = sample_list[self.elapsed_index]
            # If elapsed is valid
            if elapsed > 0.0:
                # Perfom bandwidth calculation
                bw = value/elapsed
            else:
                # In case of invalid time set transferred bytes
                bw = value
            newdata[groupname] = bw

        # Update stored data
        self.data = newdata


# Throughput Instrument for combination of measurements from two different positions
class GaugeTotalThroughput(GaugeThroughput):
    def __init__(self, *args, **kwargs):
        self.groupone = kwargs["groupone"]
        self.groupother = kwargs["groupother"]
        del kwargs["groupone"]
        del kwargs["groupother"]
        super(GaugeTotalThroughput, self).__init__(*args, **kwargs)

    def append_data(self, groupname, line, mo):
        # Do actual data analysis from line
        value = None
        if self.grouptime is not None:
            # Extract value from line
            elapsed = self.value_cast(mo.groupdict().get(self.grouptime))
            try:
                # Try to extract value one from line
                value = self.value_cast(mo.groupdict().get(self.groupone))
            except:
                try:
                    # Try to extract value other from line
                    value = self.value_cast(mo.groupdict().get(self.groupother))
                except:
                    pass

        if value is not None:
            # Get current throughput info list
            current = self.data.get(groupname, [0, 0])
            # Increment bytes transferred
            current[self.value_index] += value
            # Increment time elapsed
            current[self.elapsed_index] += elapsed

            # Update stored data
            self.data[groupname] = current


# AutoIncremental Instrument from 0 one by one
class CounterInc(Instrument):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(CounterInc, self).__init__(*args, **kwargs)

    def append_data(self, groupname, line, mo):
        # Update stored data
        self.data[groupname] = self.data.get(groupname, 0) + 1

    def read(self):
        # Return the current results of the bucket
        data_list = super(CounterInc, self).read()

        # Empty bucket and start again
        self.reset()
        return data_list


# Not tested
class CounterSum(Instrument):
    def append_data(self, groupname, line, mo):
        # Do actual data analysis from line
        minimum = self.value_cast(0)
        if self.regex_group:
            groups = mo.groupdict()
            value = self.value_cast(groups.get(self.regex_group))
        else:
            value = self.value_cast(mo.groups()[0])

        # Update stored data
        self.data[groupname] = self.data.get(groupname, minimum) + value


# Not tested
class Max(GaugeInt):
    def append_data(self, groupname, line, mo):
        # Do actual data analysis from line
        if self.regex_group:
            value = self.value_cast(mo.groupdict().get(self.regex_group))
        else:
            value = self.value_cast(mo.groups()[0])
        current = self.data.get(groupname, None)
        if value > current or current is None:
            # Update stored data
            self.data[groupname] = value


# Not tested
class DeriveCounter(CounterSum):
    def __init__(self, *args, **kwargs):
        kwargs["value_cast"] = (lambda x: int(x) % NUM32)
        super(DeriveCounter, self).__init__(*args, **kwargs)
        self.last_read = None

    def reset(self):
        # Empty bucket and start again
        super(DeriveCounter, self).reset()
        self.last_read = None

    def read(self):
        # Return the current results of the bucket
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
