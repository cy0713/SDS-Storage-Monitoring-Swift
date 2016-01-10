import os
import threading
import uuid
import re
from pygtail import Pygtail
import urlparse
import SocketServer
import Queue
import logging
import logging.handlers

logger = logging.getLogger("GROUPINGTAIL")


# Syslog handler
class SyslogUDPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = bytes.decode(self.request[0].strip().strip('\x00'))
        print data
        self.server.queue.queue.put(str(data))


# Queue file for syslog handler
class QueueFile:
    def __init__(self):
        self.queue = Queue.Queue()

    def readlines(self):
        while not self.queue.empty():
            yield self.queue.get()


# GroupingTail class that represents all matchings for a single file
class GroupingTail(object):
    def __init__(self, filepath, groupby, groupname=None):
        self.groupmatch = re.compile(groupby)
        # write an offset file so that we start somewhat at the end of the file

        # either filepath is a path or a syslogd url
        (scheme, netloc, path, params, query, fragment) = urlparse.urlparse(filepath)
        if scheme == 'syslog':
            host, port = netloc.split(':')
            self.fin = QueueFile()
            self.server = SocketServer.UDPServer((host, int(port)), SyslogUDPHandler)
            self.server.queue = self.fin

            th = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.5))
            th.setDaemon(True)
            th.start()
        else:
            # Create a temporal file with offset info
            self.offsetpath = "/tmp/" + str(uuid.uuid4())
            try:
                inode = os.stat(filepath).st_ino
                offset = os.path.getsize(filepath) - 1024
            except OSError:
                pass
            else:
                if offset > 0:
                    foffset = open(self.offsetpath, "w")
                    foffset.write("%s\n%s" % (inode, offset))
                    foffset.close()

            self.fin = Pygtail(filepath, offset_file=self.offsetpath, copytruncate=True)

        # List of matchings
        self.match_definitions = []
        # Regex group name for grouping
        self.groupbygroup = groupname

    def __del__(self):
        if hasattr(self, 'server'):
            self.server.socket.close()

    # Update method processing last lines
    def update(self):
        for line in self.fin.readlines():
            groupname = None
            mo = self.groupmatch.match(line)
            if mo is not None:
                if self.groupbygroup is None and mo.groups():
                    # No groupbygroup get first group name
                    groupname = mo.groups()[0]
                elif self.groupbygroup is not None:
                    # Get groupname from line
                    groupname = mo.groupdict().get(self.groupbygroup)

            if groupname is not None:
                # Normalize groupname
                groupname = groupname.replace(".", "_").replace("-", "_")
                # Check all possible matchings
                for match in self.match_definitions:
                    instrument = match["instrument"]
                    instrument.write(groupname, line)

    # Attatch match to groupingtail class
    def add_match(self, instance_name, valuetype, instrument):
        self.match_definitions.append(dict(
            instance_name=instance_name,
            valuetype=valuetype,
            instrument=instrument
        ))

    # Get stored values from instrument
    def read_metrics(self):
        # For all matchings
        for match in self.match_definitions:
            instance_name = match["instance_name"]
            instrument = match["instrument"]
            valuetype = match["valuetype"]

            # Get metric info
            for groupname, value in instrument.read():
                # Construct grouping name for this metric value
                metric_name = "%s*%s" % (groupname, instance_name)
                # Send metric info
                yield (metric_name, valuetype, value)
