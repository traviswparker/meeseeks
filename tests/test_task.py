import unittest
import uuid
import os
from io import StringIO
from meeseeks.task import Task
import base64
import binascii

class TestTask(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_execute_basic_command(self):
        filename = '/tmp/%s' % str(uuid.uuid4())
        job = self.job = {
            'cmd': [
                'touch',
                filename
            ]
        }
        task = Task(job) 

        # Makes me nervous...what if it never finishes?!
        while task.isAlive():
            continue

        self.assertTrue(os.path.exists(filename))

        # Finally, remove it
        try: 
            os.remove(filename)
        except Exception as e: 
            pass

    def test_can_kill_process(self):
        job = self.job = {
            'cmd': [
                'sleep',
                '300'
            ]
        }
        task = Task(job) 

        # By this time, job is already open, but just not started -- we can kill now anyway
        task.kill()

        # Then wait for result
        while task.isAlive():
            continue

        # We made it out, so it must have died
        self.assertTrue(True) 
    
    def test_stderr_output(self):
        job = self.job = {
            'cmd': [
                'sleep',
                'abc'
            ]
        }
        task = Task(job) 

        # Then wait for result
        while task.isAlive():
            continue

        try:
            base64.b64decode(task.stderr)
        except Exception as e:
            self.assertTrue(False)

        # We made it out, so it must have died
        self.assertTrue(True)
    
    def test_stderr_output_is_empty_when_valid(self):
        job = self.job = {
            'cmd': [
                'sleep',
                '1'
            ]
        }
        task = Task(job) 

        # Then wait for result
        while task.isAlive():
            continue

        # We made it out, so it must have died
        self.assertIsNone(task.stderr)

    def test_stdout_output(self):
        job = self.job = {
            'cmd': [
                'echo',
                '5'
            ]
        }
        task = Task(job) 

        # Then wait for result
        while task.isAlive():
            continue

        self.assertEqual(b'5', base64.b64decode(task.stdout).strip())

if __name__ == '__main__':
    unittest.main()