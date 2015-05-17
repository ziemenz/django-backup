import re

from django.core.management import call_command


def test_simple_backup_generation(tmpdir, settings, db):
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    call_command('backup')
    assert len(tmpdir.listdir()) == 1
    assert re.match(r'backup_\d{8}-\d{6}\.sql', tmpdir.listdir()[0].basename)


def test_compressed_backup_generation(tmpdir, settings, db):
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    call_command('backup', compress=True)
    assert len(tmpdir.listdir()) == 1
    assert re.match(r'backup_\d{8}-\d{6}\.sql\.gz',
                    tmpdir.listdir()[0].basename)


def test_backup_sftp_upload(tmpdir, settings, db, sftpserver):
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    settings.BACKUP_FTP_SERVER = '{}:{}'.format(
        sftpserver.host, sftpserver.port)
    settings.BACKUP_FTP_USERNAME = 'username'
    settings.BACKUP_FTP_PASSWORD = 'password'
    settings.BACKUP_FTP_DIRECTORY = '/backups'
    server_fs = {'backups': []}
    with sftpserver.serve_content(server_fs):
        call_command('backup', ftp=True)
        assert 1 == len(server_fs['backups'])
        # By default the backup is also kept locally.
        assert 1 == len(tmpdir.listdir())


def test_backup_sftp_upload_with_deletelocal(tmpdir, settings, db, sftpserver):
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    settings.BACKUP_FTP_SERVER = '{}:{}'.format(
        sftpserver.host, sftpserver.port)
    settings.BACKUP_FTP_USERNAME = 'username'
    settings.BACKUP_FTP_PASSWORD = 'password'
    settings.BACKUP_FTP_DIRECTORY = '/backups'
    server_fs = {'backups': []}
    with sftpserver.serve_content(server_fs):
        call_command('backup', ftp=True, deletelocal=True, delete_local=True)
        assert 1 == len(server_fs['backups'])
        assert 0 == len(tmpdir.listdir())
