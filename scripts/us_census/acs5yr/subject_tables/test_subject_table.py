"""Generic tests for subject tables"""

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

import os
import sys
import tempfile
import unittest
import subprocess
from absl import app, flags

FLAGS = flags.FLAGS
flags.DEFINE_string('table_dir', None, 'Path to subject table directory')

class TestSubjectTable(unittest.TestCase):

    def test_csv_mcf_column_map(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            pass_arg = ['python']
            pass_arg.append('process.py')
            pass_arg.append('--table_prefix=test')
            pass_arg.append(f'--spec_path={_SPEC_PATH}')
            pass_arg.append(f'--input_path={_ZIP_PATH}')
            pass_arg.append(f'--output_dir={tmp_dir}')
            pass_arg.append(f'--has_percent=True')
            pass_arg.append(f'--debug=False')

            subprocess.run(pass_arg)

            test_mcf_path = os.path.join(tmp_dir, 'test_output.mcf')
            test_column_map_path = os.path.join(tmp_dir, 'column_map.json')
            test_csv_path = os.path.join(tmp_dir, 'test_cleaned.csv')

            with open(test_mcf_path) as mcf_f:
                mcf_result = mcf_f.read()
                with open(_STATVAR_MCF_PATH) as expected_mcf_f:
                    expected_mcf_result = expected_mcf_f.read()
                    self.assertEqual(mcf_result, expected_mcf_result)

            with open(test_column_map_path) as cmap_f:
                cmap_result = cmap_f.read()
                with open(_COLUMN_MAP_PATH) as expected_cmap_f:
                    expected_cmap_result = expected_cmap_f.read()
                    self.assertEqual(cmap_result, expected_cmap_result)

            with open(test_csv_path) as csv_f:
                csv_result = csv_f.read()
                with open(_CSV_PATH) as expected_csv_f:
                    expected_csv_result = expected_csv_f.read()
                    self.assertEqual(csv_result, expected_csv_result)

def main(argv):
    _TABLE_DIR = flags.FLAGS.table_dir
    _SPEC_PATH = None
    _STATVAR_MCF_PATH = None
    _CSV_PATH = None
    _COLUMN_MAP_PATH = None
    _ZIP_PATH = None

    # Find spec, Expects spec to be in _TABLE_DIR
    for filename in os.listdir(_TABLE_DIR):
        if '_spec.json' in filename:
            _SPEC_PATH = os.path.join(_TABLE_DIR, filename)
    assert _SPEC_PATH is not None

    # Expect statvar MCF, column map, csv and zip to be in testdata folder
    testdata_path = os.path.join(_TABLE_DIR, 'testdata')
    for filename in os.listdir(testdata_path):
        if '_cleaned.csv' in filename:
            _CSV_PATH = os.path.join(testdata_path, filename)
        elif '_output.mcf' in filename:
            _STATVAR_MCF_PATH = os.path.join(testdata_path, filename)
        elif 'column_map.json' in filename:
            _COLUMN_MAP_PATH = os.path.join(testdata_path, filename)
        elif filename.endswith('.zip'):
            _ZIP_PATH = os.path.join(testdata_path, filename)

    assert _CSV_PATH is not None
    assert _STATVAR_MCF_PATH is not None
    assert _COLUMN_MAP_PATH is not None
    assert _ZIP_PATH is not None

    return _SPEC_PATH, _CSV_PATH, _STATVAR_MCF_PATH, _COLUMN_MAP_PATH, _ZIP_PATH

if __name__ == '__main__':
    app.run(main)
    unittest.main()