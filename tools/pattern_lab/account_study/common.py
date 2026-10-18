"""Small strict JSON and filesystem primitives for account evidence."""
from pathlib import Path
import hashlib
import json
import os
import re
import unicodedata

from ..accounts.contracts import require


def closed(value, required, optional=()):
    require(type(value) is dict and set(required) <= set(value) <= set(required) | set(optional),
            'missing or unknown object keys: ' + str(required))
    return value


def json_text(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(',', ':'), allow_nan=False)


def parse_json(text):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON key: ' + key)
            result[key] = value
        return result
    def invalid(value):
        require(False, 'nonfinite JSON: ' + value)
    return json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)


def read_json(path):
    return parse_json(Path(path).read_text(encoding='utf-8'))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json_text(value) + '\n', encoding='utf-8')
    os.replace(temporary, path)


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def digest_id(value):
    require(isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value), 'invalid evidence identity')
    return value


def relative_name(name):
    require(isinstance(name, str) and name and '\\' not in name and ':' not in name,
            'invalid member path')
    parts = name.split('/')
    for part in parts:
        require(part not in ('', '.', '..') and not part.endswith((' ', '.')) and
                not any(ord(c) < 32 for c in part) and not re.search(r'[<>"|?*]', part), 'unsafe member path')
        require(not re.fullmatch(r'(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?', part, re.I),
                'reserved Windows member')
    require(unicodedata.normalize('NFC', name) == name, 'noncanonical Unicode path')
    return name


def member_names(names):
    seen, directories, aliases = set(), set(), {}
    for name in names:
        folded = relative_name(name).casefold()
        require(folded not in seen, 'duplicate/casefold member collision')
        seen.add(folded)
        parts = folded.split('/')
        directories.update('/'.join(parts[:i]) for i in range(1, len(parts)))
        original = name.split('/')
        for i in range(1, len(original)+1):
            prefix = '/'.join(original[:i])
            require(aliases.get(prefix.casefold(), prefix) == prefix, 'casefold directory alias')
            aliases[prefix.casefold()] = prefix
    require(not seen & directories, 'file/directory member collision')


def fresh_directory(path, source=None):
    path = Path(path).expanduser().resolve()
    require(not path.exists(), 'output must be a new directory')
    if source is not None:
        source = Path(source).expanduser().resolve()
        require(not path.is_relative_to(source) and not source.is_relative_to(path), 'overlapping input/output')
    path.mkdir(parents=True)
    return path
