import tempfile
import shutil
import os

from django.core.management import call_command
from django.test import TestCase


class DbBackupTests(TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_generation(self):
        with self.settings(BACKUP_LOCAL_DIRECTORY=self.tmpdir):
            call_command('backup')
            files = os.listdir(self.tmpdir)
            self.assertEqual(1, len(files))
            self.assertRegexpMatches(files[0], r'backup_\d{8}-\d{6}\.sql')

    def test_compressed_generation(self):
        with self.settings(BACKUP_LOCAL_DIRECTORY=self.tmpdir):
            call_command('backup', compress=True)
            files = os.listdir(self.tmpdir)
            self.assertEqual(1, len(files))
            self.assertRegexpMatches(files[0], r'backup_\d{8}-\d{6}\.sql\.gz')
