*PENQUINS - Processing ENormous Queries of ztf Users INStantaneously*

This tutorial will demonstrate how to programmatically query the database collections with `python >3.6`.
<br>

#### Installation

Install the client library [penquins.py](https://github.com/dmitryduev/kowalski/blob/master/penquins.py), 
with `pip` into your environment:

```bash
pip install git+https://github.com/dmitryduev/kowalski.git
```

`penuquins` is very lightweight and only depends on `pymongo` and `requests`. 
<br>

#### Quick start

Now you can import the library in your script:

```python
from penquins import Kowalski
```

Define your access credentials:

```python
username = 'YOUR_USERNAME'
password = 'YOUR_PASSWORD'
```

Connect to `kowalski`:

```python
k = Kowalski(username=username, password=password, verbose=False)
```

<span class="badge badge-secondary">Note</span> `Kowalski` object is a context manager, so can be used with a `with` statement:

```python
with Kowalski(username=username, password=password, verbose=False) as k:
    # do stuff
    pass
```

Set `verbose=True` if you want more feedback from Kowalski.

Now let us construct a simple query and run it. _Please refer to the documentation for more info 
on available query types and how to (efficiently) construct them_. 

```python
qu = {"query_type": "general_search", "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})"}

r = k.query(query=qu)
print(r)
```

This query will block the execution of your program until it receives the result or when it hits the default timeout,
which is set to _five hours_. You can manually set up the query timeout in seconds:

```python
r = k.query(query=qu, timeout=1)
```

Starting from `penquins` version `1.0.0`, queries are no longer registered in the database and saved to disk _by default_,
which provides significant execution speed improvement.
This behavior can be overridden:

```python
qu = {"query_type": "general_search", 
      "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
      "kwargs": {"save": True}}
```

Executing this query will also block the execution of your program. You can enqueue a query on the server like this:

```python
qu = {"query_type": "general_search", 
      "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
      "kwargs": {"enqueue_only": True}}

r = k.query(query=qu)
qid = r['query_id']
```

Executing this query will return a query id that can be then used to retrieve the query result:

```python
result = k.get_query(query_id=qid, part='result')
```

You can also retrieve the original query:

```python
result = k.get_query(query_id=qid, part='result')
```

Or delete the query from Kowalski:

```python
result = k.delete_query(query_id=qid)
```

By default, the queries/results stored on `kowalski` are deleted after five days. 
To override this, set a manual expiration interval in days:

```python
qu = {"query_type": "general_search", 
      "query": "db['ZTF_alerts'].find_one({}, {'_id': 1})",
      "kwargs": {"query_expiration_interval": 30}}
```
<br>

#### Error management

<span class="badge badge-secondary">Note</span> `kowalski` will refuse connection if your installed version of `penquins` is outdated.

In case a query fails, the result will contain the traceback error message. Using our running example query, if you, 
for example, made a typo in the query like this (forgot the bracket at the end):

```python
qu = {"query_type": "general_search", "query": "db['ZTF_alerts'].find_one({}, {'_id': 1}"}
r = k.query(query=q)
# print the error message:
print(r['result']['message'])
```

You will get an error message that will read something like:

```
Traceback (most recent call last):
  File "server.py", line 755, in execute_query
    _select = eval(qq)  
  File "", line 1
    db['ZTF_alerts'].find_one({}, {'_id': 1}
                                           ^
SyntaxError: unexpected EOF while parsing
```
<br>

#### Migrating from `penquins==0.4.x`

The main things to keep in mind when migrating from an older version of `penquins`:

- Queries are not saved to db/disk by default anymore. 
- Filter/projection syntax in cone searches
- query_sync deprecated, use query
