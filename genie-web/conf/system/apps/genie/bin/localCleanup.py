#!/usr/bin/env python2.7

##
#
#  Copyright 2013 Netflix, Inc.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
##

#
# Clean up local file systems on any genie node.
# It should be run as root.
#
# Potential future enhancements:
# - Clean up some directories more or less aggressively based on free space in their file systems.
# - Clean up different file systems in parallel.
# - Clean up /tmp, including removing subdirectories if emptied.

import datetime
import glob
import os
import subprocess
import sys
import threading


# for entries that cover overlapping sets of files, their relative order is important
dirsToClean = (
   # dirGlob, maxDepth, fileNamePattern, compressAfterDays, removeAfterDays
    ('/mnt/logs/apache/archive',        None,   None,       None,   7),
    ('/mnt/logs/tomcat/archive',        None,   None,       None,   7),
    ('/mnt/tomcat/genie-jobs',          None,   None,       None,   3),
    ('/mnt/logs/genie',                 None,   None,       None,   3),
    ('/mnt/var/lib/hadoop/tmp',         None,   None,       None,   3),
    ('/mnt/logs/hadoop',                None,   None,       None,   3),
)


def log(msg, *args, **kwargs):
    logTime = datetime.datetime.utcnow().isoformat(' ')
    threadName = threading.currentThread().getName()
    msgPrefix = '[%s] [%s]' % (logTime, threadName)

    assert not (args and kwargs)
    if args:
        msg %= args
    elif kwargs:
        msg %= kwargs

    print msgPrefix, msg
    sys.stdout.flush()


def runFind(dir, maxDepth, fileNamePattern, days, execParams):
    cmd = ['find', dir]
    if maxDepth is not None:
        cmd += ['-maxdepth', str(maxDepth)]
    if fileNamePattern is not None:
        cmd += ['-name', fileNamePattern]
    cmd += [
        '-type', 'f',
        '-daystart', '-mtime', '+%s' % days,
        '(',
        '-exec', 'lsof', '{}', ';',
        '-o',
        '-exec',
    ]
    cmd += execParams
    cmd += [
        ';',
        ')',
    ]

    log('Executing: %s', ' '.join(cmd))
    subprocess.check_call(cmd, bufsize=1, close_fds=True)


for dirGlob, maxDepth, fileNamePattern, compressAfterDays, removeAfterDays in dirsToClean:
    for dir in glob.glob(dirGlob):
        if os.path.exists(dir):
            log('Cleaning up %s...', dir)
        else:
            log('Skipping nonexistent %s...', dir)
            continue

        assert maxDepth is None or maxDepth > 0, maxDepth
        if compressAfterDays and removeAfterDays:
            assert compressAfterDays < removeAfterDays, (compressAfterDays, removeAfterDays)

       # remove before compress
        if removeAfterDays > 0:
            runFind(dir, maxDepth, fileNamePattern, removeAfterDays,
                    ['rm', '-v', '{}'])

        if compressAfterDays > 0:
            runFind(dir, maxDepth, fileNamePattern, compressAfterDays,
                    ['gzip', '-v', '--best', '{}'])

