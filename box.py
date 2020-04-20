import os
import io
import sys
from multiprocessing.dummy import Pool
import pandas as pd
from boxsdk import JWTAuth, OAuth2, Client

from libs.config import LoadSettings

config = LoadSettings()

default_cache = config['root']['cache']
default_config = config['config_files']['box']

class LifespanBox:
    def __init__(self, cache=default_cache, user='Lifespan Automation', config_file=default_config):
        self.user = user
        self.cache = cache
        self.config_file = config_file
        if not os.path.exists(cache):
            os.mkdir(cache)
        self.client = self.get_client()

    def get_client(self):
        auth = JWTAuth.from_settings_file(self.config_file)
        admin_client = Client(auth)

        lifespan_user = None
        # lifespan_user = client.create_user('Lifespan Automation')
        for user in admin_client.users():
            if user.name == self.user:
                lifespan_user = user

        if not lifespan_user:
            print(self.user + ' user was not found. Exiting...')
            sys.exit(-1)

        return admin_client.as_user(lifespan_user)

    def get_dev_client(self):
        # Dev access token, active for 1 hour. Get new token here:
        # https://wustl.app.box.com/developers/console
        auth = OAuth2(
            client_id='',
            client_secret='',
            access_token=''
        )
        return Client(auth)

    def list_of_files(self, folders, extension='.csv', recursively=True):
        """
        A legacy function.
        """
        result = {}

        for folder_id in folders:

            f = self.client.folder(folder_id)
            #print('Scanning %s' % folder_id)
            print('.', end='')
            items = list(f.get_items())

            folders = []
            files = {}

            for i in items:
                if i.type == 'file':
                    if i.name.endswith(extension):
                        files[i.id] = {
                            'filename': i.name,
                            'fileid': i.id,
                            'sha1': i.sha1
                        }
                elif i.type == 'folder':
                    folders.append(i.id)

            result.update(files)
            if recursively:
                result.update(self.list_of_files(folders, extension, True))

        return result

    def folder_info(self, folder_id):
        f = self.client.folder(folder_id=str(folder_id)).get()
        print('folder owner: ' + f.owned_by['login'])
        print('folder name: ' + f['name'])

    def get_files(self, folder_id, pattern=None, maxfiles=None):
        """ Gets all files in a folder matching pattern up to maxfiles
        :pattern - Can be any string and can contain '*' for wildcards
        :maxfiles - May return slightly more than this due to the offset value
            and pattern matching
        """
        limit = 1000
        if maxfiles and maxfiles < limit:
            limit = maxfiles
        offset = 0
        root_folder = self.client.folder(folder_id=str(folder_id))
        files = []

        while True:
            items = root_folder.get_items(limit=limit, offset=offset)

            for f in items:
                if f.type != 'file':
                    continue
                if not pattern:
                    files.append(f)
                elif self._match(f.name, pattern):
                    files.append(f)

            # We either exhausted the listing or have reached maxfiles
            if not items:
                break
            if maxfiles and len(files) >= maxfiles:
                break

            offset += limit

        return files

    def search(
            self,
            pattern,
            limit=100,
            maxresults=1000,
            exclude=None,
            ancestor_folders=None,
            file_extensions=None):
        """
        Extends box search to narrow down based on glob like pattern
        Exclusions can be specified as comma separated string, like 'Not,This'
        """
        offset = 0
        results = []

        print('looking for "{}" ...'.format(pattern))
        result = self.client.search().query(
            pattern,
            limit=limit,
            offset=offset,
            ancestor_folders=ancestor_folders,
            file_extensions=file_extensions)
        results.extend(result)

        matches = []

        for r in results:
            match = True
            for substr in pattern.split('*'):
                if substr not in r.name:
                    match = False
            if match:  # and exclude and exclude not in r.name:
                if not exclude:
                    matches.append(r)
                else:
                    exclusions = exclude.split(',')
                    included = True
                    for exclusion in exclusions:
                        if exclusion in r.name:
                            included = False
                    if included:
                        matches.append(r)

        return matches

    def getFileById(self, fileId):
        """
        Get box file instance.
        To get content use `.content()`.
        Other metadata is available via `.get()`.
        """
        return self.client.file(file_id=str(fileId))

    def readFile(self, fileId):
        """Bypasses the local filesystem, returns an inmemory file handle"""
        file = self.getFileById(fileId)
        return io.BytesIO(file.content())

    def read_csv(self, fileId):
        """ Read a csv file into a pandas dataframe, without storing a cached version."""
        return pd.read_csv(self.readFile(fileId))

    def read_excel(self, fileId):
        """ Read an excel file into a pandas dataframe, without storing a cached version."""
        return pd.read_excel(self.readFile(fileId))

    def read_text(self, fileId):
        """ Read a text file into a string, without storing a cached version."""
        f = self.getFileById(fileId).content()

        try:
            return f.decode('UTF-16')
        except UnicodeDecodeError:
            return f.decode('UTF-8')

    def downloadFile(self, fileId, download_dir=None, override_if_exists=False):
        """
        Downloads a single file to cache space or provided directory
        """
        download_dir = download_dir or self.cache

        file = self.getFileById(fileId)
        path = os.path.join(download_dir, file.get().name)

        if os.path.exists(path) and not override_if_exists:
            return path

        with open(path, "wb+") as fd:
            fd.write(file.content())

        return path


    def download_file(self, file_id):
        """
        Downloads a single file to cache space or provided directory
        """

        f = self.client.file(file_id=str(file_id))
        print(f.get().name)
        file_path = os.path.join(self.cache, f.get().name)

        with open(file_path, 'wb') as out:
            out.write(f.content())

        return f

    def download_files(self, file_ids, directory=None, workers=20):
        """
        Takes a list of file ids and downloads them all to cache space or user
        specified directory
        """
        if directory:
            self.cache = directory
        pool = Pool(workers)
        filepaths = pool.map(self.downloadFile, file_ids)
        pool.close()
        pool.join()
        return filepaths
        # Euivalent to this for loop
        # for f in file_ids:
        #     self.download_file(f)

    def upload_file(self, source_path, folder_id):
        """
        Upload a new file into an existing folder by folder_id.
        """
        file = self.client.folder(str(folder_id)).upload(source_path)
        print(file)
        return file

    def update_file(self, file_id, file_path, rename=True):
        """
        Alias of `update_version`.
        """
        return self.update_version(file_id, file_path, rename)

    def update_version(self, file_id, file_path, rename=True):
        """
        Upload a new version of an existing file by file_id
        """
        base = os.path.basename(file_path)
        file = self.client.file(str(file_id))
        f = file.update_contents(file_path)

        if rename and file.get().name != base:
            file.rename(base)

        return f

    @staticmethod
    def _match(string, pattern):
        match = True
        for substr in pattern.split('*'):
            # Skip "empty" matches
            if not substr:
                continue

            if substr not in string:
                # print(substr)
                match = False
        return match

    def Box2dataframe(self, curated_fileid_start):
        """
        A Legacy function.
        """

        # get current best curated data from BOX (a csv with one header row)
        # and read into pandas dataframe for QC
        raw_fileid = curated_fileid_start
        data_path = box.downloadFile(raw_fileid)
        raw = pd.read_csv(
            data_path,
            header=0,
            low_memory=False,
            encoding='ISO-8859-1')
        # raw['DateCreatedDatetime']=pd.to_datetime(raw.DateCreated).dt.round('min')
        # raw['InstStartedDatetime']=pd.to_datetime(raw.InstStarted).dt.round('min')
        # raw['InstEndedDatetime']=pd.to_datetime(raw.InstEnded).dt.round('min')
        return raw


if __name__ == '__main__':
    box = LifespanBox()

    results = box.search(
        pattern='-Aging_scores.csv',
        limit=10,
        maxresults=10
    )
    for r in results[0:49]:
        print(r)
    print('^ {} results'.format(len(results)))
