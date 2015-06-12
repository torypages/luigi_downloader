from stat import S_ISDIR
import paramiko
from os.path import join
import luigi
from luigi.contrib import sftp
from abc import ABCMeta, abstractmethod



# from .luigi.luigi.contrib import sftp



# from .luigi import luigi

ssh = None
sftpcon = None
hostname = None
username = None
password = None

class Lister(luigi.Task):
    __metaclass__ = ABCMeta

    @abstractmethod
    def list_files(self):
        raise NotImplementedError



class FileTransfer(luigi.Task):
    remote_path = luigi.Parameter()

    def input(self):
        return sftp.SftpTarget(path=self.remote_path, host=hostname, username=username, password=password)

    def output(self):
        return luigi.LocalTarget("roar.txt")

    def run(self):
        print("Wanting to get {}".format(self.remote_path))
        #TODO Just copy the stuff in the most basic way possible.
        # I bet this is a bad way to do this.
        out = self.output().open('w')
        for i in self.input().open('r'):
            out.write(i)



class SftpLister(Lister):

    def exists(self):
        return False

    def requires(self):
        # Need to prepend mtime stamp to
        return [FileTransfer(remote_path=i) for i in self.list_files('/home/sftptest/')]


    def is_dir(self, path):
        try:
            return S_ISDIR(sftpcon.stat(path).st_mode)
        except IOError:
            # Not a file or dir, does not exist at all.
            return False



    def list_files(self, the_path):
        directory_contents = sftpcon.listdir(the_path)
        rtrn = []
        for afile in directory_contents:
            # ignore hidden for now, maybe allow later. Custom filter somewhere
            if afile.startswith('.'):
                continue
            afile = join(the_path, afile)
            if self.is_dir(afile):
                rtrn.extend(self.list_files(join(the_path, afile)))
            else:
                rtrn.append(afile)
        return rtrn




if __name__ == "__main__":
    import os

    with open('sftpcredentials.txt', 'r') as f:
        hostname = f.readline().strip()
        username = f.readline().strip()
        password = f.readline().strip()

    # s = sftp.RemoteFileSystem(
    #     host=hostname,
    #     username=username,
    #     password=password
    # )

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname,
                username=username,
                password=password,
                port=22)
    sftpcon = ssh.open_sftp()
    a = SftpLister()
    # print('-----')
    # print(a.list_files("/home/sftptest/"))


    luigi.run(["--local-scheduler"], main_task_cls=SftpLister)
