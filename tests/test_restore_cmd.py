import datetime
from django.core.management import call_command
from django.contrib.auth.models import User


def test_full_roundtrip(db, tmpdir, settings, sftpserver):
    """
    This does a basic roundtrip without actually checking the restoration
    itself.
    """
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    settings.RESTORE_FROM_FTP_DIRECTORY = '/backups'
    settings.BACKUP_FTP_SERVER = '{}:{}'.format(
        sftpserver.host, sftpserver.port)
    settings.BACKUP_FTP_USERNAME = 'username'
    settings.BACKUP_FTP_PASSWORD = 'password'
    settings.BACKUP_FTP_DIRECTORY = '/backups'
    server_fs = {'backups': {}}

    call_command('backup')

    backup_file = [f for f in tmpdir.listdir()
                   if f.basename.startswith('backup_')][0]
    server_fs['backups'][backup_file.basename] = backup_file.read()
    with sftpserver.serve_content(server_fs):
        call_command('restore')


def test_simple_restore(db, sftpserver, settings, tmpdir):
    """
    In this test we restore a simple backup on top of an already existing
    database.
    """
    settings.BACKUP_LOCAL_DIRECTORY = str(tmpdir)
    settings.RESTORE_FROM_FTP_DIRECTORY = '/backups'
    settings.BACKUP_FTP_SERVER = '{}:{}'.format(
        sftpserver.host, sftpserver.port)
    settings.BACKUP_FTP_USERNAME = 'username'
    settings.BACKUP_FTP_PASSWORD = 'password'
    settings.BACKUP_FTP_DIRECTORY = '/backups'
    server_fs = {'backups': {}}
    backup_file_name = 'backup_{}.sql'.format(
        datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    )
    backup_data = '''INSERT INTO auth_user (id, username, first_name, last_name, email, password, is_staff, is_active, is_superuser, last_login, date_joined) VALUES (4, 'test', 'Tester', 'Test', 'test@test.com', 'pbkdf2_sha256$10000$Wgz0Lavtdp42$QKv6th80A30rfRTdoI5gXUV9sOGHf07i/FZiy3l25/g=', false, true, false, '2012-05-28 15:04:32.360306+02', '2012-05-28 15:04:06.835383+02');'''
    server_fs['backups'][backup_file_name] = backup_data
    with sftpserver.serve_content(server_fs):
        call_command('restore')
        User.objects.get(username='test')
