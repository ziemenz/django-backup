import datetime
import pytest
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
    server_fs = {'backups': {}}
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
    server_fs = {'backups': {}}
    with sftpserver.serve_content(server_fs):
        call_command('backup', ftp=True, deletelocal=True, delete_local=True)
        assert 1 == len(server_fs['backups'])
        assert 0 == len(tmpdir.listdir())


def test_backup_with_media(tmpdir, settings, db):
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    call_command('backup', media=True)
    assert len(tmpdir.listdir()) == 2
    assert len([f for f in tmpdir.listdir() if f.basename.startswith('dir_')]) == 1


def test_surplus_local_db_removal_without_setting(tmpdir, settings, db):
    """
    If the user requests a cleanup but forgets to set the
    BACKUP_DATABASE_COPIES setting, a keyerror should be raised.
    """
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    old_files = set([
        'backup_20140101-000000.sql',
        'backup_20140102-000000.sql',
    ])
    for f in old_files:
        tmpdir.join(f).write('')
    with pytest.raises(AttributeError):
        call_command('backup', clean_local_db=True, cleanlocaldb=True,
                     media=False)


def test_surplus_local_db_removal(tmpdir, settings, db):
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    # We wont to keep one backup for today apart from the one that
    # will be created with the backup command right now.
    settings.BACKUP_DATABASE_COPIES = {
        'monthly': 0,
        'weekly': 0,
        'daily': 1,
    }
    todays_file = 'backup_{}-010000.sql'.format(
        datetime.datetime.now().strftime('%Y%m%d'))
    old_files = set([
        'backup_20140101-010000.sql',
        'backup_20140102-010000.sql',
        todays_file,
    ])
    for f in old_files:
        tmpdir.join(f).write('')
    call_command('backup', clean_local_db=True, cleanlocaldb=True, media=False)
    found_files = set([f.basename for f in tmpdir.listdir()])
    assert todays_file in found_files
    assert (old_files - set([todays_file])).isdisjoint(found_files)


def test_surplus_remote_db_removal(tmpdir, settings, db, sftpserver):
    settings.BACKUP_FTP_SERVER = '{}:{}'.format(
        sftpserver.host, sftpserver.port)
    settings.BACKUP_FTP_USERNAME = 'username'
    settings.BACKUP_FTP_PASSWORD = 'password'
    settings.BACKUP_FTP_DIRECTORY = '/backups'
    server_fs = {'backups': {}}
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    settings.BACKUP_DATABASE_COPIES = {
        'monthly': 0,
        'weekly': 0,
        'daily': 0,
    }
    old_files = set([
        'backup_20140101-000000.sql',
        'backup_20140102-000000.sql',
    ])
    for f in old_files:
        tmpdir.join(f).write('')
        server_fs['backups'][f] = ""
    with sftpserver.serve_content(server_fs):
        call_command('backup', clean_remote_db=True, cleanremotedb=True,
                     media=False, ftp=True)
        assert set(server_fs['backups'].keys()).isdisjoint(old_files)


def test_surplus_media_removal(tmpdir, settings, db, sftpserver):
    settings.BACKUP_FTP_SERVER = '{}:{}'.format(
        sftpserver.host, sftpserver.port)
    settings.BACKUP_FTP_USERNAME = 'username'
    settings.BACKUP_FTP_PASSWORD = 'password'
    settings.BACKUP_FTP_DIRECTORY = '/backups'
    server_fs = {'backups': {}}
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    settings.BACKUP_MEDIA_COPIES = {
        'monthly': 0,
        'weekly': 0,
        'daily': 1,
    }
    todays_file = 'dir_{}-010000.sql'.format(
        datetime.datetime.now().strftime('%Y%m%d'))
    old_files = set([
        'dir_20140101-000000.tar.gz',
        'dir_20140102-000000.tar.gz',
        todays_file,
    ])
    for f in old_files:
        tmpdir.join(f).write('')
        server_fs['backups'][f] = ""
    removed_files = (old_files - set([todays_file]))
    with sftpserver.serve_content(server_fs):
        call_command('backup', clean_remote_media=True, cleanremotemedia=True,
                     clean_local_media=True, cleanlocalmedia=True,
                     media=True, ftp=True)
        assert set([f.basename for f in tmpdir.listdir()]).isdisjoint(
            removed_files)
        assert todays_file in [f.basename for f in tmpdir.listdir()]
        assert set(server_fs['backups'].keys()).isdisjoint(removed_files)
