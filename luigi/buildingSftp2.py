# from .contrib.sftp import SftpTarget
from luigi.contrib.sftp import RemoteFileSystem
from datetime import datetime

s = RemoteFileSystem(
    # path='/home/sftptest',
    host='localhost',
    username='sftptest',
    password='36344A285DA465BD49F96E6C69ED'
)


# print(s.exists('/home/sftptest/iexist.txt'))
# print(s.exists('/home/sftptest/idontexist.txt'))
# print(s.exists('/home/sftptest/iexist.txt', datetime(2016, 1, 1)))


s._rm_recursive(None, '/home/sftptest/adir')



print("cheese")
