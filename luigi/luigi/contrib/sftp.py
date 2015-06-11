# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This library is a wrapper of ftplib.
It is convenient to move data from/to FTP.

There is an example on how to use it (example/ftp_experiment_outputs.py)

You can also find unittest for each class.

Be aware that normal ftp do not provide secure communication.
"""

import paramiko

import datetime
import ftplib
import os
import sys
import random
import io

import luigi
import luigi.file
import luigi.format
import luigi.target
from luigi.format import FileWrapper, MixedUnicodeBytes
from stat import S_ISDIR

class RemoteFileSystem(luigi.target.FileSystem):

    def __init__(self, host, username=None, password=None, port=22):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.ssh = None
        self.sftpcon = None

    def _connect(self):
        """
        Log in to sftp.
        """

        self.ssh = paramiko.SSHClient()
        # TODO: Probably want to make this an option.
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=self.host,
                    username=self.username,
                    password=self.password,
                    port=self.port)
        self.sftpcon = self.ssh.open_sftp()


    def exists(self, path, mtime=None):
        """
        Return `True` if file or directory at `path` exist, False otherwise.

        Additional check on modified time when mtime is passed in.

        Return False if the file's modified time is older mtime.
        """
        self._connect()
        to_return = False
        # files = self.sftpcon.listdir
        try:
            the_file = self.sftpcon.stat(path)
            if not mtime:
                to_return = True
            else:
                modified = datetime.datetime.fromtimestamp(the_file.st_mtime)
                to_return = modified > mtime
        # TODO: is python3 except style cool?
        except FileNotFoundError as e:
            to_return = False

        return to_return

    def remove(self, ftp, path):
        """
        Recursively delete a directory tree on a remote server.

        Assumes that a file or folder does exist at path.

        Remove file or directory at location ``path``.

        :param path: a path within the FileSystem to remove.
        :type path: str

        Inspired by: http://stackoverflow.com/a/20507586/4621195
        """

        def isdir(path):
            try:
                return S_ISDIR(self.sftpcon.stat(path).st_mode)
            except IOError:
                # Not a file or dir, does not exist at all.
                return False

        def rm(path):
            files = self.sftpcon.listdir(path=path)

            for f in files:
                filepath = os.path.join(path, f)
                if isdir(filepath):
                    rm(filepath)
                else:
                    self.sftpcon.remove(filepath)

            self.sftpcon.rmdir(path)

        self._connect()

        if not isdir(path):
            # If it is a file, just remove it.
            self.sftpcon.remove(path)
            return

        rm(path)

        # TODO sftp QUIT?


    def put(self, local_path, path):
        # TODO NEED TO TEST, RUN
        # create parent folder if not exists
        self._connect()

        normpath = os.path.normpath(path)
        folder = os.path.dirname(normpath)

        # create paths if do not exists
        print('folder', folder)
        for subfolder in folder.split(os.sep):
            if subfolder:
                try:
                    self.sftpcon.stat(subfolder)
                except IOError:
                    print('mkdir', subfolder)
                    self.sftpcon.mkdir(subfolder)

            # print('chdir', subfolder)
            # self.sftpcon.chdir(subfolder)

        # go back to ftp root folder
        # self.sftpcon.path()

        # random file name
        # TODO should check to make sure this doesn't exist first.
        tmp_path = folder + os.sep + 'luigi-tmp-%09d' % random.randrange(0, 1e10)

        print('local_path', local_path)
        print('tmp_path', tmp_path)
        self.sftpcon.put(local_path, tmp_path)
        self.sftpcon.rename(tmp_path, normpath)

        # TODO
        # self.ftpcon.quit()

    def get(self, path, local_path):
        # TODO CURRENTLY WORKING HERE
        # Create folder if it does not exist
        normpath = os.path.normpath(local_path)
        folder = os.path.dirname(normpath)
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        # TODO use a proper temp file.
        tmp_local_path = local_path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
        # download file
        self._connect()
        self.sftpcon.get(path, tmp_local_path)
        # self.ftpcon.quit()

        os.rename(tmp_local_path, local_path)


class AtomicFtpFile(luigi.target.AtomicLocalFile):
    """
    Simple class that writes to a temp file and upload to ftp on close().

    Also cleans up the temp file if close is not invoked.
    """

    def __init__(self, fs, path):
        """
        Initializes an AtomicFtpfile instance.
        :param fs:
        :param path:
        :type path: str
        """
        self._fs = fs
        super(AtomicFtpFile, self).__init__(path)

    def move_to_final_destination(self):
        self._fs.put(self.tmp_path, self.path)

    @property
    def fs(self):
        return self._fs


class SftpTarget(luigi.target.FileSystemTarget):
    """
    Target used for reading from remote files.

    The target is implemented using ssh commands streaming data over the network.
    """

    def __init__(self, path, host, format=None, username=None, password=None,
                 port=21, mtime=None, tls=False):
        if format is None:
            format = luigi.format.get_default_format()

        # Allow to write unicode in file for retrocompatibility
        if sys.version_info[:2] <= (2, 6):
            format = format >> MixedUnicodeBytes

        self.path = path
        self.mtime = mtime
        self.format = format
        self.tls = tls
        self._fs = RemoteFileSystem(host, username, password, port, tls)

    @property
    def fs(self):
        return self._fs

    def open(self, mode):
        """
        Open the FileSystem target.

        This method returns a file-like object which can either be read from or written to depending
        on the specified mode.

        :param mode: the mode `r` opens the FileSystemTarget in read-only mode, whereas `w` will
                     open the FileSystemTarget in write mode. Subclasses can implement
                     additional options.
        :type mode: str
        """
        if mode == 'w':
            return self.format.pipe_writer(AtomicFtpFile(self._fs, self.path))

        elif mode == 'r':
            self.__tmp_path = self.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
            # download file to local
            self._fs.get(self.path, self.__tmp_path)

            return self.format.pipe_reader(
                FileWrapper(io.BufferedReader(io.FileIO(self.__tmp_path, 'r')))
            )
        else:
            raise Exception('mode must be r/w')

    def exists(self):
        return self.fs.exists(self.path, self.mtime)

    def put(self, local_path):
        self.fs.put(local_path, self.path)

    def get(self, local_path):
        self.fs.get(self.path, local_path)
