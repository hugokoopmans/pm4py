import io
import csv
import logging
import pandas as pd
import itertools as it
import requests
import time
from datetime import timezone

def _urljoin(url1, url2):
    return f"{url1.rstrip('/')}/{url2.lstrip('/')}"

def get_columns(entity_set, excluded_columns=(), excluded_datatypes=()):
    return [col for col in entity_set._entity_set.entity_type.proprties() 
            if col.name not in excluded_columns and col.typ.name not in excluded_datatypes]

def select_all(entity_set):    
    return entity_set.get_entities().select(','.join(p.name for p in get_columns(entity_set)))

def column_definition(col):
    definition = [f'"{col.name.upper()}"']
    if   col.typ.name == 'Edm.Byte':
        definition.append('DECIMAL(3,0)')
    elif col.typ.name == 'Edm.Int16':
        definition.append('DECIMAL(5,0)')
    elif col.typ.name == 'Edm.Int32':
        definition.append('DECIMAL(9,0)')
    elif col.typ.name == 'Edm.Int64':
        definition.append('DECIMAL(18,0)')
    elif col.typ.name == 'Edm.Float':
        definition.append('DOUBLE')
    elif col.typ.name == 'Edm.Double':
        definition.append('DOUBLE')    
    elif col.typ.name == 'Edm.Boolean':
        definition.append('BOOLEAN')
    elif col.typ.name == 'Edm.DateTime':
        definition.append('TIMESTAMP WITH LOCAL TIME ZONE')
    elif col.typ.name == 'Edm.Guid':
        definition.append('CHAR(36)')
    elif col.typ.name == 'Edm.Binary':
        logging.warning('Edm.Binary type not fully supported by Exasol')
        definition.append('VARCHAR(200000)')
    else:
        definition.append('VARCHAR(200000)')
    if not col.nullable:
        definition.append('NOT NULL')
    return ' '.join(definition)

def table_definition(entity_set, name=None, excluded_columns=(), excluded_datatypes=()):
    columns = get_columns(entity_set, excluded_columns=excluded_columns, excluded_datatypes=excluded_datatypes)
    name = name or entity_set._name
    sep = ',\n\t'
    return f'''CREATE TABLE {name} (\n\t{sep.join(column_definition(col) for col in columns)})'''

_MAX_ATTEMPTS = 10
def retry_request(connection, *args, **kwargs):
    last_error = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            response = connection.get(*args, **kwargs)
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            last_error = e
            delay = min(0.1 * (10 ** attempt), 10)
            logging.warning('HTTP GET failed. Waiting %dms before retry. Retrying %s/%s... with exception e=%s reponse=%s', int(delay * 1000), attempt + 1, _MAX_ATTEMPTS, e, e.response.content.decode('utf8', errors='backslashreplace'))
            time.sleep(delay)
        except Exception as e:
            last_error = e
            delay = min(0.1 * (10 ** attempt), 10)
            logging.warning('Unexpected error. Waiting %dms before retry. Retrying %s/%s... with exception e=%s', int(delay * 1000), attempt + 1, _MAX_ATTEMPTS, repr(e))
            time.sleep(delay)
    raise RuntimeError(f'Connection unsuccessful after {attempt} retries; Skipping; Last error={repr(last_error)}')

def paginate(request):
    url = _urljoin(request._url, request.get_path())
    params  = request.get_query_params()
    headers = request.get_headers()
    while url:
        response = retry_request(request._connection, url, params=params, headers=headers)
        yield from request._handler(response)
        try:
            url = response.json()['d']['__next']
        except:
            url = None
        finally:
            params = None # Parameters only required once

def _chunked_dataframe(iterable, chunksize, *args, **kwargs):
    iterator = iter(iterable)
    while True:
        df = pd.DataFrame.from_records(it.islice(iterator, chunksize), *args, **kwargs)
        if len(df) == 0: break
        yield df

def read_odata(query, *args, chunksize=None, **kwargs):
    data = (r._cache for r in paginate(query))
    if chunksize is not None:
        return _chunked_dataframe(data, chunksize, *args, **kwargs)
    else:
        return pd.DataFrame(data, *args, **kwargs)

def data_conversion(col):
    if col.typ.name == 'Edm.Boolean':
        return int
    elif col.typ.name == 'Edm.DateTime':
        return lambda x: x.astimezone(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f') # OData returns timestamps with UTC timezone by default.
    else:
        return lambda x: x
    
def import_from_odata(pipe, query, *, chunksize=1000, data_conversion=data_conversion, inline_transform=None, **kwargs):
    columns = query._select.split(',')
    column_properties   = { p.name : p for p in query._entity_type.proprties() }
    if data_conversion:
        transformations = { col : data_conversion(column_properties[col]) for col in columns}
    else: transformations = None
    
    wrapped_pipe = io.TextIOWrapper(pipe, newline='\n', encoding='utf-8')
    
    for df in read_odata(query, chunksize=chunksize):
        if transformations:
            df = df.transform(transformations)
        if inline_transform:
            df = inline_transform(df)
        df.to_csv(wrapped_pipe, header=False, index=False, mode='wb', line_terminator='\n', quoting=csv.QUOTE_NONNUMERIC, **kwargs)