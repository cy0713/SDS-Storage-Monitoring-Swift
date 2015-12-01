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


class SyslogUDPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        data = bytes.decode(self.request[0].strip().strip('\x00'))
        print data
        self.server.queue.queue.put(str(data))


class QueueFile:
    def __init__(self):
        self.queue = Queue.Queue()

    def readlines(self):
        while not self.queue.empty():
            yield self.queue.get()


class GroupingTail(object):
    def __init__(self, filepath, groupby, groupname=None):
        self.groupmatch = re.compile(groupby)
        logger.info("groupingtail.groupingtail.constructor %s groupby %s groupmatch %s\n" % (
                        filepath, groupby, self.groupmatch.pattern))
        # write an offset file so that we start somewhat at the end of the file

        # either filepath is a path or a syslogd url
        (scheme, netloc, path, params, query, fragment) = urlparse.urlparse(filepath)
        if scheme == 'syslog':
            logger.info("groupingtail.groupingtail.constructor.syslog %s\n")
            host, port = netloc.split(':')
            self.fin = QueueFile()
            self.server = SocketServer.UDPServer((host, int(port)), SyslogUDPHandler)
            self.server.queue = self.fin

            th = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.5))
            th.setDaemon(True)
            th.start()
        else:
            self.offsetpath = "/tmp/" + str(uuid.uuid4())
            logger.info("groupingtail.groupingtail.constructor.file %s\n" % self.offsetpath)
            # print self.offsetpath
            try:
                inode = os.stat(filepath).st_ino
                offset = os.path.getsize(filepath) - 1024
                # print inode
                # print offset
            except OSError:
                pass
            else:
                if offset > 0:
                    # print 'write offset'
                    foffset = open(self.offsetpath, "w")
                    foffset.write("%s\n%s" % (inode, offset))
                    foffset.close()

            self.fin = Pygtail(filepath, offset_file=self.offsetpath, copytruncate=True)

        self.match_definitions = []
        self.groupbygroup = groupname

    def __del__(self):
        if hasattr(self, 'server'):
            self.server.socket.close()

    def update(self):
        logger.info("groupingtail.groupingtail.update\n")
        for line in self.fin.readlines():
            groupname = None
            mo = self.groupmatch.match(line)
            logger.info("groupingtail.groupingtail.update mo %s line %s\n", mo, line)
            if mo is not None:
                if self.groupbygroup is None and mo.groups():
                    groupname = mo.groups()[0]
                elif self.groupbygroup is not None:
                    groupname = mo.groupdict().get(self.groupbygroup)
            logger.info("groupingtail.groupingtail.update groupname %s mo %s line %s\n", groupname, mo, line)
            if groupname is not None:
                groupname = groupname.replace(".", "_").replace("-", "_")
                for match in self.match_definitions:
                    logger.info("groupingtail.groupingtail.update match %s groupname %s mo %s line %s\n", match,
                                groupname, mo, line)
                    instrument = match["instrument"]
                    instrument.write(groupname, line)

    def add_match(self, instance_name, valuetype, instrument):
        self.match_definitions.append(dict(
            instance_name=instance_name,
            valuetype=valuetype,
            instrument=instrument
        ))

    def read_metrics(self):
        logger.info("groupingtail.groupingtail.read_metrics len match_definitions %d\n" % len(self.match_definitions))
        for match in self.match_definitions:
            instance_name = match["instance_name"]
            instrument = match["instrument"]
            valuetype = match["valuetype"]

            logger.info(
                "groupingtail.groupingtail.read_metrics match %s %s %s\n" % (instance_name, instrument, valuetype))

            for groupname, value in instrument.read():
                metric_name = "%s*%s" % (groupname, instance_name)
                logger.info(
                    "groupingtail.groupingtail.read_metrics metric read %s %s %s\n" % (metric_name, valuetype, value))
                yield (metric_name, valuetype, value)
