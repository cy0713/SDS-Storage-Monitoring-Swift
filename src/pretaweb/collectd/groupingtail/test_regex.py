import re

#
# Small program to test new matchings and new instruments without running all
# collectd system
#


# Grouping regex
groupby = "^\\S+\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\w+\\s+\\/v1\\/AUTH_(?P<tenant>\\w+)\\/(?P<container>[-\\w\\.]+)[\\/\\S\\s]+"
# Regex group name
groupbygroup = "container"


regex_group = "get_bytes"
regex = ".+ GET [\\s\\S]+(?P<get_bytes>\\d+)\\s+\\S+\\s+tx\\w+[\\s\\S]+"
lines = [
    "Dec  2 17:19:27 swift_mdw proxy-server: 10.30.235.235 10.30.235.235 02/Dec/2015/16/19/27 GET /v1/AUTH_4f0279da74ef4584a29dc72c835fe2c9/ HTTP/1.0 200 - python-swiftclient-2.3.1 4a49577fae884452... - 2 - tx3de6f87d0fa7499c9386d-00565f1a0f - 0.0449 - - 1449073167.457756042 1449073167.502661943 2" ,
    "Dec  3 10:41:10 swift_mdw proxy-server: - - 03/Dec/2015/09/41/10 HEAD /v1/AUTH_4f0279da74ef4584a29dc72c835fe2c9 HTTP/1.0 204 - Swift - - - - txe6e922bee7e7475c88735-0056600e36 - 0.0108 RL - 1449135670.136400938 1449135670.147243023 -"
         ]

groupmatch = re.compile(groupby)
test = re.compile(regex)

data = {}


def append_data(groupname, line, mo):
    if regex_group:
        value = mo.groupdict().get(regex_group)
    else:
        value = mo.groups()[0]

    data[groupname] = value


def reset():
    data = {}


for line in lines:
    groupname = None
    mo = groupmatch.match(line)
    if mo is not None:
        if groupbygroup is None and mo.groups():
            groupname = mo.groups()[0]
        elif groupbygroup is not None:
            groupname = mo.groupdict().get(groupbygroup)
    if groupname is not None:
        groupname = groupname.replace(".", "_").replace("-", "_")
        mo = test.match(line)
        if mo is not None:
            try:
                append_data(groupname, line, mo)
            except ValueError:
                pass
            except Exception as e:
                # the instrument failed.
                reset()
            else:
                print "touch_group"
        else:
            print "No fa matching"
