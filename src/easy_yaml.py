import yaml
import re

from .memoizable import Memoizable, sha256

split_array = re.compile('^- ', re.MULTILINE)
split_dict = re.compile('^(\S+:)', re.MULTILINE)

class EasyYaml(Memoizable):
    def __init__(self, cache_file='.yaml_cache'):
        super().__init__(cache_file=cache_file, hashfunc=sha256)

    def execute(self, path):
        with open(path, 'r') as f:
            return yaml.load(f, yaml.SafeLoader)

    def write(self, filename, obj):
        with open(filename, 'w') as fd:
            y = yaml.dump(obj, default_flow_style=False, sort_keys=False)
            sum_array = sum([1 for _ in split_array.finditer(y)])
            sum_dict = sum([1 for _ in split_dict.finditer(y)])
            y = split_dict.sub('\n\\1', y) if sum_array < sum_dict else split_array.sub('\n- ', y)
            fd.write(y.strip())

