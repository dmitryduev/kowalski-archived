In this tutorial, I will demonstrate how to programmatically query the database collection with `python3.6`.

You may want to create a python virtual environment with `conda` and install the required python modules into it:

```bash
# let's call it penquins
# PENQUINS - Processing ENormous Queries of ztf Users INStantaneously
conda create --name penquins python=3.6
# type y to proceed
# install pip:
conda install -n penquins pip
# activate the environment:
source activate penquins
# install the client library:
pip install git+https://github.com/dmitryduev/broker.git
```

Alternatively, install the client library [penquins.py](https://github.com/dmitryduev/broker/blob/master/penquins.py), 
(together with dependencies) with `pip` into your environment:

```bash
pip install git+https://github.com/dmitryduev/broker.git
```

Now you can import the library in your script. Let us also import `numpy` and the `as_completed` function from 
`python3`'s standard library `concurrent.futures` for the purposes of this tutorial.

```python
from penquins import Kowalski
import numpy as np
from concurrent.futures import as_completed
```

Define your access credentials:

```python
username = 'YOUR_USERNAME'
password = 'YOUR_PASSWORD'
```

Initialize your connection to the server:

```python
s = Kowalski(username=username, password=password)
```

Set `verbose=True` if you want more feedback from Kowalski.

>Internally, the server communicates with the `penquins.Kowalski` objects using the `socket.io` events.
>In particular, it will emit an `'enqueued'` event upon successful enqueuing of a query,
>and a `'finished'` event once a query has been processed. If it fails to enqueue a query,
>an `'msg'` event will be emitted containing a brief description of the issue.
>The server will assign a unique `id` to each successfully enqueued query that can be used
>to retrieve the query result. The result is stored in the database and can be retrieved 
>within 30 days upon completion.

Now let us construct a cone search query and submit it. _Please refer to other tutorials for more info 
on available query types and how to (efficiently) construct them_. 

```python
q = {"query_type": "cone_search",
     "object_coordinates": {
         "radec": "[(173.5155088, 33.0845502)]", "cone_search_radius": "8",
         "cone_search_unit": "arcsec"
     },
     "catalogs": {
         "ZTF_alerts": {
             "filter": {},
             "projection": {
                 "objectId": 1,
                 "candidate.rcid": 1,
                 "candidate.ra": 1
             }
         }
     }
     }

r = s.query(query=q)
print(r)
```

Note that this call will block the execution of your program until it receives the result.

In case a query fails, the result will contain the traceback error message. Let us demonstrate that. The following
query returns the real-bogus score for the ZTF alert with `objectId` `ZTF18aabcyiy`:

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'objectId': {'$eq': 'ZTF18aabcyiy'}}, {'_id': 0, 'candidate.rb': 1})"
     }
r = s.query(query=q)
print(r)
```

This is what the query result should look like:

```python
{'result': {'query_result': [{'candidate': {'rb': 0.3766666650772095}}]}, 'task_id': '382956d6a0d33c36d64c6326948a9c39'}
```

Now if we, for example, made a typo in the query like this (forgot the bracket at the end):

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'objectId': {'$eq': 'ZTF18aabcyiy'}}, {'_id': 0, 'candidate.rb': 1}"
     }
r = s.query(query=q)
# print the error message:
print(r['result']['msg'])
```

This is what the error message will read something like:

```
Traceback (most recent call last):
  File "/data/ztf/dev/broker/src/penquins/tasks.py", line 148, in query_db
    _select = eval(qq)
  File "<string>", line 1
    db['ZTF_alerts'].find({'objectId': {'$eq': 'ZTF18aabcyiy'}}, {'_id': 0, 'candidate.rb': 1}
                                                                                             ^
SyntaxError: unexpected EOF while parsing
```

**Once you're done, close the connection:**

```python
s.close()
```

Alternatively, you can use the `python` `with` statement with the `Kowalski` object, which will take care of
closing the connection:

```python
with Kowalski(username=username, password=password) as s:
    r = s.query(query=q)
```

<br>

###### Auxiliary stuff

By default, a query/result will expire and be deleted in 30 days. If you want to set a custom expiration interval
in days, do it like this:

```python
q = {"query_type": "general_search",
     "query": "db['ZTF_alerts'].count()",
     "kwargs": {"query_expiration_interval": 1}
     }
```
