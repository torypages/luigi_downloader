from stat import S_ISDIR
import paramiko
from os.path import join
from luigi import luigi
from luigi.luigi.contrib import sftp
ssh = None
sftpcon = None

class Lister(luigi.target):
    def list_files(self):
        raise NotImplementedError



class FileTransfer(sftp.SftpTarget):
    remote_path = luigi.Parameter()

    def input(self):
        return sftp.SftpTarget(self.remote_path)

    def output(self):
        return luigi.LocalTarget()

    def run(self):
        #TODO Just copy the stuff in the most basic way possible.
        # I bet this is a bad way to do this.
        out = open(self.output(), 'w')
        for i in open(self.input(), 'r'):
            out.write(i)



class SftpLister(Lister):

    def requires(self):
        # Need to prepend mtime stamp to
        return [FileTransfer(remote_path=i) for i in self.list_files()]


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
            afile = join(the_path, afile)
            if self.is_dir(afile):
                rtrn.extend(self.list_files(join(the_path, afile)))
            else:
                rtrn.append(afile)
        return rtrn




if __name__ == "__main__":
    import os

    print(os.getcwd())
    with open('sftpcredentials.txt', 'r') as f:
        hostname = f.readline().strip()
        username = f.readline().strip()
        password = f.readline().strip()

    # s = sftp.RemoteFileSystem(
    #     host=hostname,
    #     username=username,
    #     password=password
    # )

    print("hey", "there")
    print(hostname, username, password)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname,
                username=username,
                password=password,
                port=22)
    sftpcon = ssh.open_sftp()
    a = SftpLister()
    print('-----')
    print(a.list_files("/home/sftptest/"))
