def hashable(item):
    """Determine whether `item` can be hashed."""
    try:
        hash(item)
    except TypeError:
        return False
    return True

def sha256(filename):
    """
    Digest a file using sha256
    """
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        # Read and update hash string value in blocks of 4K
        while chunk := f.read(8192):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def __not_equal__(a, b):
    return a != b


class Memoizable:
    def __init__(self, cache_file='.cache', expire_in_days=7, hashfunc=None):
        self.cache_file = cache_file
        self.cache = {}
        self.expire_in = expire_in_days * 60 * 60 * 24
        if hashfunc is not None:
            self.current_stamp = hashfunc
            self.expiration_stamp = None
            self.is_expired = __not_equal__
        self.load_cache()

    def __call__(self, *args):
        if not hashable(args):
            print("Uncacheable args.", args)
            return self.execute(*args)

        cached = self.cache.get(args, None)
        current = self.current_stamp(*args)
        if cached is None or self.is_expired(cached[1], current):
            value = self.execute(*args)
            if self.expiration_stamp is not None:
                current = self.expiration_stamp(*args)
            self.cache[args] = value, current
            self.save_cache()
            return value
        else:
            return copy.deepcopy(self.cache[args][0])

    def load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'rb') as fd:
                self.cache = pickle.load(fd)
        else:
            self.cache = {}

    def save_cache(self, cache_file=None):
        if cache_file is None: cache_file = self.cache_file
        with open(cache_file, 'wb') as f:
            pickle.dump(self.cache, f)

    def execute(self, *args):
        raise Exception("Executor not yet defined.")
        return False

    def is_expired(self, cached, current):
        return current > cached

    def current_stamp(self, *args):
        return time.time()

    def expiration_stamp(self, *args):
        return time.time() + self.expire_in


