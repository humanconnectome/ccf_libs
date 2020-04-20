import numpy as np
import io
import pandas as pd
import requests

from libs.config import LoadSettings

config = LoadSettings()['Redcap']
default_url = config['api_url']


class RedcapTable:
    def __init__(self, token, url = None):
        self.url = url if url else default_url
        self.token = token

    @staticmethod
    def get_table_by_name(name):
        """
        Create a new instance of RedcapTable by name. Table must exist in the config datasources.
        """
        if name not in config['datasources']:
            raise Exception(name + ' is not available, try one of ', list(config['datasources'].keys()))

        ds = config['datasources'][name]
        url = ds.get('url', default_url)
        return RedcapTable(ds['token'], url)

    def post(self, payload):
        """
        Internal function. Sends POST request to redcap server.
        """
        data = payload.copy()
        data['token'] = self.token
        r = requests.post(self.url, data)
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
        if fields:
            data['fields[]'] = fields

        if forms:
            data['forms[]'] = forms

        r = self.post(data)
        r = io.BytesIO(r.content)
        return pd.read_csv(r, encoding='utf8', low_memory=False)

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
        if fields:
            data['fields[]'] = fields

        if events:
            data['events[]'] = events

        if forms:
            data['forms[]'] = forms

        r = self.post(data)
        r = io.BytesIO(r.content)
        return pd.read_csv(r, encoding='utf8', parse_dates=True, low_memory=False)

    def send_frame(self, dataframe, overwrite=True):
        r = self.post({
            'content': 'record',
            'format': 'csv',
            'type': 'flat',
            'overwriteBehavior': 'overwrite' if overwrite else 'normal',
            'data': dataframe.to_csv(index=False),
            'returnContent': 'ids',
            'returnFormat': 'json',
        })
        return r

    def delete_records(self, records):
        """
        Delete a list of records
        """
        if not isinstance(records, list):
            records = [records]

        r = self.post({
            'action': 'delete',
            'content': 'record',
            'records[]': records
        })
        return r

    def generate_next_record_ids(self, count=1):
        """
        If the current table is set to autogenerate new IDs, this function
        will return the id of the next record.
        """
        n = int(self.post({'content': 'generateNextRecordName'}).content)
        return list(range(n, n+count))


def get_behavioral_ids(keep_parents=False):
    dfs = [ get_behavioral(study) \
             for study in config['behavioral'].keys() \
                 if study != 'hcpdparents' or keep_parents
          ]

    return pd.concat(dfs, sort=False, ignore_index=True)

def get_behavioral(study, fields=None, keep_withdrawn=False):
    if study not in config['behavioral']:
        #throw Exception('This study is not available. ' + study)
        print('Error', 'This study is not available.', study)
        return {}

    s = config['behavioral'][study]
    fieldnames = s['fields']
    events = s['events']
    table = RedcapTable(s['token'])
    fields = fields.copy() if fields else []
    fields += list(fieldnames.values())
    df = table.get_frame(fields, events)
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

def get_full(study, keep_withdrawn=False):
    s = config['behavioral'][study]
    table = RedcapTable(s['token'])
    df = table.get_frame(events = s['events'])
    fieldnames = s['fields']
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
