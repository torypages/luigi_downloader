from luigi.contrib import sftp
import os

print(os.getcwd())
with open('sftpcredentials.txt', 'r') as f:
    hostname = f.readline().strip()
    username = f.readline().strip()
    password = f.readline().strip()

s = sftp.RemoteFileSystem(
    host=hostname,
    username=username,
    password=password
)

print("Debug")

