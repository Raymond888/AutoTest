#!/usr/bin/python
# -*- coding: utf-8 -*-


import paramiko


class BaseConnector(object):
    def __init__(self, username, password, host, port, *args, **kwargs):
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        return NotImplemented


class SshConnector(BaseConnector):

    def __init__(self, username, password, host, port, *args, **kwargs):
        super(SshConnector, self).__init__(username, password, host, port, *args, **kwargs)

    def connect(self):
        pass


class ParamikoConnector(BaseConnector):

    def __init__(self, username, password, host, port=None, rsa=None, *args, **kwargs):
        super(ParamikoConnector, self).__init__(username, password, host, port, *args, **kwargs)
        self.rsa = rsa

    def __del__(self):
        self.ssh.close()
        self.trans.close()

    def ssh_client(self, trans=True):
        self.ssh = paramiko.SSHClient()

        if trans:
            self.ssh._transport = self.transport()
        else:
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(hostname=self.host,
                             port=self.port,
                             username=self.username,
                             password=self.password
                             )
        return self.ssh

    def ssh_execute(self, statement):
        '''stdin, stdout, stderr = ssh.exec_command('ls')'''
        stdin, stdout, stderr = self.ssh_client().exec_command(statement)
        res, err = stdout.read(), stderr.read()
        result = res if res else err
        return result.decode()

    def transport(self):
        # pkey = paramiko.RSAKey.from_private_key_file(self.rsa, password=self.password)
        self.trans = paramiko.Transport(self.host, 22)
        # self.trans.connect(username=self.username, pkey=pkey)
        self.trans.connect(username=self.username, password=self.password)
        return self.trans

    @property
    def sftp(self):
        sftp = paramiko.SFTPClient.from_transport(self.transport())
        return sftp

    def sftp_upload(self, local_path, remote_path):
        self.sftp.put(local_path, remote_path)

    def sftp_download(self, remote_path, local_path):
        self.sftp.get(remote_path, local_path)



