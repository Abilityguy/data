# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A script to process FBI Hate Crime table 3 publications."""
import os
import sys
import tempfile
import csv
import json
import pandas as pd

# Allows the following module imports to work when running as a script
_SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_SCRIPT_PATH,
                             '../../../../util/'))  # for statvar_dcid_generator

from statvar_dcid_generator import get_statvar_dcid

YEAR_INDEX = 0

# Columns in final cleaned CSV
OUTPUT_COLUMNS = ('Year', 'StatVar', 'Quantity')

# A config that maps the year to corresponding xls file with args to be used
# with pandas.read_excel()
YEARWISE_CONFIG = {
    '2020': {
        'type': 'xls',
        'path': '../source_data/2020/table_5.xlsx',
        'args': {
            'header': 6,
            'skipfooter': 2,
            'usecols': list(range(0, 14))
        }
    },
    '2019': {
        'type': 'xls',
        'path': '../source_data/2019/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 2,
            'usecols': list(range(0, 14))
        }
    },
    '2018': {
        'type': 'xls',
        'path': '../source_data/2018/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 2,
            'usecols': list(range(0, 14))
        }
    },
    '2017': {
        'type': 'xls',
        'path': '../source_data/2017/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 2,
            'usecols': list(range(0, 14))
        }
    },
    '2016': {
        'type': 'xls',
        'path': '../source_data/2016/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 2,
            'usecols': list(range(0, 14))
        }
    },
    '2015': {
        'type': 'xls',
        'path': '../source_data/2015/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 2,
            'usecols': list(range(0, 14))
        }
    },
    '2014': {
        'type': 'xls',
        'path': '../source_data/2014/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 3,
            'usecols': list(range(0, 14))
        }
    },
    '2013': {
        'type': 'xls',
        'path': '../source_data/2013/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 3,
            'usecols': list(range(0, 14))
        }
    },
    '2012': {
        'type': 'xls',
        'path': '../source_data/2012/table_5.xls',
        'args': {
            'header': 5,
            'skipfooter': 1
        }
    },
    '2011': {
        'type': 'xls',
        'path': '../source_data/2011/table_5.xls',
        'args': {
            'header': 4,
            'skipfooter': 1
        }
    },
    '2010': {
        'type': 'xls',
        'path': '../source_data/2010/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    },
    '2009': {
        'type': 'xls',
        'path': '../source_data/2009/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    },
    '2008': {
        'type': 'xls',
        'path': '../source_data/2008/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    },
    '2007': {
        'type': 'xls',
        'path': '../source_data/2007/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    },
    '2006': {
        'type': 'xls',
        'path': '../source_data/2006/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    },
    '2005': {
        'type': 'xls',
        'path': '../source_data/2005/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    },
    '2004': {
        'type': 'xls',
        'path': '../source_data/2004/table_5.xls',
        'args': {
            'header': 3,
            'skipfooter': 1
        }
    }
}


def _create_csv_mcf(csv_files: list, cleaned_csv_path: str,
                    config: dict) -> list:
    """Creates StatVars according to values in csv_files and write the final
    output to a csv.

    Args:
        csv_files: A list of CSV file paths to process.
        cleaned_csv_path: Path of the final cleaned CSV file.
        config: A dict which maps constraint props to the statvar based on
          values in the CSV. See scripts/fbi/hate_crime/table2/config.json for
          an example.

    Returns:
        A list of statvars.
    """
    statvars = []
    with open(cleaned_csv_path, 'w') as output_f:
        writer = csv.DictWriter(output_f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for csv_file in csv_files:
            with open(csv_file, 'r') as input_f:
                reader = csv.DictReader(input_f)
                statvars_list = _write_output_csv(reader, writer, config)
                statvars.extend(statvars_list)
    return statvars


def _update_statvars(statvar_list: list, key_value: dict):
    """Given a list of statvars and a key:value pair, this functions adds the
    key value pair to each statvar.
    """
    for d in statvar_list:
        d.update(key_value)


def _update_statvar_dcids(statvar_list: list, config: dict):
    """Given a list of statvars, generates the dcid for each statvar after
    accounting for dependent PVs.
    """
    for d in statvar_list:
        ignore_props = _get_dpv(d, config)
        dcid = get_statvar_dcid(d, ignore_props=ignore_props)
        d['Node'] = dcid


def _write_row(year: int, statvar_dcid: str, quantity: str,
               writer: csv.DictWriter):
    """A wrapper to write data to the cleaned CSV."""
    processed_dict = {
        'Year': year,
        'StatVar': statvar_dcid,
        'Quantity': quantity
    }
    writer.writerow(processed_dict)


def _get_dpv(statvar: dict, config: dict) -> list:
    """A function that goes through the statvar dict and the config and returns
    a list of properties to ignore when generating the dcid.

    Args:
        statvar: A dictionary of prop:values of the statvar
        config: A dict which maps constraint props to the statvar based on
          values in the CSV. See scripts/fbi/hate_crime/table2/config.json for
          an example. The 'dpv' key is used to identify dependent properties.

    Returns:
        A list of properties to ignore when generating the dcid
    """
    ignore_props = []
    for spec in config['dpv']:
        if spec['cprop'] in statvar:
            dpv_prop = spec['dpv']['prop']
            dpv_val = spec['dpv']['val']
            if dpv_val == statvar.get(dpv_prop, None):
                ignore_props.append(dpv_prop)
    return ignore_props


def _write_output_csv(reader: csv.DictReader, writer: csv.DictWriter,
                      config: dict) -> list:
    """Reads each row of a CSV and creates statvars for counts of
    Incidents, Offenses, Victims and Known Offenders with different bias
    motivations.

    Args:
        reader: CSV dict reader.
        writer: CSV dict writer of final cleaned CSV.
        config: A dict which maps constraint props to the statvar based on
          values in the CSV. See scripts/fbi/hate_crime/table2/config.json for
          an example.

    Returns:
        A list of statvars.
    """
    statvars = []
    columns = list(reader.fieldnames)
    columns.remove('bias motivation')
    columns.remove('Year')
    for crime in reader:
        bias_motivation = crime['bias motivation']
        bias_motivation_key_value = config['pvs'][bias_motivation]

        statvar_list = []
        for c in columns:
            statvar = {**config['populationType'][c]}
            statvar_list.append(statvar)

        _update_statvars(statvar_list, bias_motivation_key_value)
        _update_statvar_dcids(statvar_list, config)

        for idx, c in enumerate(columns):
            _write_row(crime['Year'], statvar_list[idx]['Node'], crime[c],
                       writer)

        statvars.extend(statvar_list)

    return statvars


def _create_mcf(stat_vars: list, mcf_file_path: str):
    """Writes all statvars to a .mcf file."""
    dcid_set = set()
    final_mcf = ''
    for sv in stat_vars:
        statvar_mcf_list = []
        dcid = sv['Node']
        if dcid in dcid_set:
            continue
        dcid_set.add(dcid)
        for p, v in sv.items():
            if p != 'Node':
                statvar_mcf_list.append(f'{p}: dcs:{v}')
        statvar_mcf = 'Node: dcid:' + dcid + '\n' + '\n'.join(statvar_mcf_list)
        final_mcf += statvar_mcf + '\n\n'

    with open(mcf_file_path, 'w') as f:
        f.write(final_mcf)


def _clean_dataframe(df: pd.DataFrame, year: str):
    """Clean the column names and offense type values in a dataframe."""
    if year in ['2020', '2019', '2018', '2017', '2016', '2015', '2014', '2013']:
        df.columns = [
            'bias motivation', 'total offenses', 'white', 'black or african american', 
            'american indian or alaska native', 'asian', 
            'native hawaiian or other pacific islander', 'multiple races', 
            'unknown race', 'hispanic or latino', 'not hispanic or latino', 
            'group of multiple ethnicities', 'unknown ethnicity', 'unknown offender' 
        ]
    else:  # 2012, 2011, 2010, 2009, 2008, 2007, 2006, 2005, 2004
        df.columns = [
            'bias motivation', 'total offenses', 'white',
            'black or african american', 'american indian or alaska native',
            'asian or pacific islander', 'multiple races', 'unknown race',
            'unknown offender'
        ]
    
    df['bias motivation'] = df['bias motivation'].replace(r'[\d:]+',
                                                          '',
                                                          regex=True)
    df['bias motivation'] = df['bias motivation'].replace(r'\s+',
                                                          ' ',
                                                          regex=True)
    df['bias motivation'] = df['bias motivation'].str.strip()
    return df



if __name__ == '__main__':
    csv_files = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for year, config in YEARWISE_CONFIG.items():
            xls_file_path = os.path.join(_SCRIPT_PATH, config['path'])
            csv_file_path = os.path.join(tmp_dir, year + '.csv')

            read_file = pd.read_excel(xls_file_path, **config['args'])
            read_file = _clean_dataframe(read_file, year)
            read_file.insert(YEAR_INDEX, 'Year', year)
            read_file.to_csv(csv_file_path, header=True, index=False)
            csv_files.append(csv_file_path)

        config_path = os.path.join(_SCRIPT_PATH, 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        cleaned_csv_path = os.path.join(_SCRIPT_PATH, 'cleaned.csv')
        statvars = _create_csv_mcf(csv_files, cleaned_csv_path, config)

        mcf_path = os.path.join(_SCRIPT_PATH, 'output.mcf')
        _create_mcf(statvars, mcf_path)
