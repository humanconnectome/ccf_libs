import os
import re
import numpy as np
import io
import pandas as pd
import requests
from .memoizable import Memoizable

from .easy_yaml import EasyYaml
from .config import LoadSettings

config = LoadSettings()['Redcap']
default_url = config['api_url']
Y = EasyYaml(".redcap_cache")


def convert_to_number(x):
    try:
        return int(x)
    except ValueError:
        try:
            return float(x)
        except ValueError:
            return x


redcap_choices_regex = re.compile(r'([^,]+), *([^|]+?)(?: *\||$)')
split_regex = re.compile(r'^(\S)', re.MULTILINE)


def choices(string):
    return {
        convert_to_number(k.strip()): v.strip()
        for k, v in redcap_choices_regex.findall(string)
    }


new_col_names = ['name', 'form', 'section', 'type', 'label', 'choices', 'note', 'validation', 'val_min', 'val_max',
                 'identifier', 'branching', 'required', 'alignment', 'qnum', 'matrix', 'matrix_rank', 'annotation']


def to_dict(df):
    df.columns = new_col_names
    elements = df.set_index('name').to_dict('index')
    newlist = {}
    for name, e in elements.items():
        # delete NaN
        e = {k: v for k, v in e.items() if pd.notna(v)}

        # split choices
        if e.get('type') in ['checkbox', 'radio', 'dropdown']:
            e['choices'] = choices(e['choices'])

        newlist[name] = e
    return newlist


class RedcapTable:
    def __init__(self, token, url=None, name=None):
        self.url = url if url else default_url
        self.token = token
        self.name = name if name else 'Unknown'

    @staticmethod
    def get_table_by_name(name):
        """
        Create a new instance of RedcapTable by name. Table must exist in the config datasources.
        """
        if name not in config['datasources']:
            raise Exception(name + ' is not available, try one of ', list(config['datasources'].keys()))

        ds = config['datasources'][name]
        url = ds.get('url', default_url)
        return RedcapTable(ds['token'], url, name)

    def __post(self, payload):
        """
        Internal function. Sends POST request to redcap server.
        """
        data = payload.copy()
        data['token'] = self.token
        r = requests.post(self.url, data)
        if r.status_code != 200:
            raise Exception("%s: Problem with the request." % self.name, r.json())
        return r

    def get_datadictionary(self, fields=None, forms=None):
        """
        Download the datadictionary for the current table.
        """
        data = {
            'format': 'csv',
            'content': 'metadata',
            'returnFormat': 'json',
        }
        if fields: data['fields[]'] = fields
        if forms: data['forms[]'] = forms

        r = self.__post(data)
        r = io.BytesIO(r.content)
        return pd.read_csv(r, encoding='utf8', low_memory=False)

    def download_datadictionary(self, directory="./definitions/", fields=None, forms=None):
        """
        Download datadictionary as yaml to directory
        :param directory:
        :param fields:
        :param forms:
        :return:
        """
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        results = self.get_datadictionary(fields, forms)
        filename = os.path.join(directory, self.name + '.yaml')
        Y.write(filename, to_dict(results))

    def get_frame(self, fields=None, events=None, forms=None):
        """
        Request the data from Redcap straight into a Pandas DataFrame.
        """
        data = {
            'format': 'csv',
            'content': 'record',
            'type': 'flahot',
            'returnFormat': 'json',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
        }
        if fields: data['fields[]'] = fields
        if events: data['events[]'] = events
        if forms: data['forms[]'] = forms

        r = self.__post(data)
        r = io.BytesIO(r.content)
        return pd.read_csv(r, encoding='utf8', parse_dates=True, low_memory=False)

    def send_frame(self, dataframe, overwrite=True):
        r = self.__post({
            'content': 'record',
            'format': 'csv',
            'type': 'flat',
            'overwriteBehavior': 'overwrite' if overwrite else 'normal',
            'data': dataframe.to_csv(index=False),
            'returnContent': 'ids',
            'returnFormat': 'json',
        })
        return r

    def delete_records(self, recordIds):
        """
        Delete a list of records
        """
        if not isinstance(recordIds, list):
            recordIds = [recordIds]

        r = self.__post({
            'action': 'delete',
            'content': 'record',
            'records[]': recordIds
        })
        return r

    def generate_next_record_ids(self, count=1):
        """
        If the current table is set to autogenerate new IDs, this function
        will return the id of the next record.
        """
        n = int(self.__post({'content': 'generateNextRecordName'}).content)
        return list(range(n, n + count))


class CachedRedcap(Memoizable):
    def __init__(self, cache_file='.redcap_cache', expire_in_days=7):
        super().__init__(cache_file=cache_file, expire_in_days=expire_in_days)

    def run(self, table_name, fields=None, events=None, forms=None):
        return RedcapTable.get_table_by_name(table_name).get_frame(fields, events, forms)

    def get_behavioral_ids(self, keep_parents=False):
        dfs = [self.get_behavioral(study) \
               for study in config['behavioral'].keys() \
               if study != 'hcpdparents' or keep_parents
               ]

        return pd.concat(dfs, sort=False, ignore_index=True)

    def get_behavioral(self, study, fields=None, keep_withdrawn=False):
        if study not in config['behavioral']:
            # throw Exception('This study is not available. ' + study)
            print('Error', 'This study is not available.', study)
            return {}

        s = config['behavioral'][study]
        fieldnames = s['fields']
        events = s['events']
        list_of_fields = None
        if fields != False:
            list_of_fields = list(fieldnames.values())
            if fields is None:
                pass
            elif type(fields) is list:
                list_of_fields += fields
            elif type(fields) is str:
                list_of_fields.append(fields)
            else:
                raise TypeError("Not sure what to do with specified fields.", fields)
        df = self.__call__(study, fields=list_of_fields, events=events)
        df.rename(columns={
            fieldnames['interview_date']: 'interview_date',
            fieldnames['field']: 'subjectid'
        }, inplace=True)
        df = df[df.subjectid.notna() & (df.subjectid != '')]
        split_df = df.subjectid.str.split("_", 1, expand=True)
        df['subject'] = split_df[0].str.strip()
        df['flagged'] = split_df[1].str.strip()
        df['study'] = study

        if not keep_withdrawn:
            df = df[df.flagged.isna()]

        interview_date = pd.to_datetime(df.interview_date)
        dob = pd.to_datetime(df.dob)
        # interview age (in months, capped at 90 y.o.)
        interview_age = (interview_date - dob) / np.timedelta64(1, 'M')
        interview_age = interview_age.apply(np.floor).astype('Int64')
        interview_age = interview_age.mask(interview_age > 1080, 1200)
        df['interview_age'] = interview_age

        if 'gender' in df.columns:
            df.gender = df.gender.replace({1: 'M', 2: 'F'})

        return df

    def get_full(self, study, keep_withdrawn=False):
        return self.get_behavioral(study, False, keep_withdrawn)
