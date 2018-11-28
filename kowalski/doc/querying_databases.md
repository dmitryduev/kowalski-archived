This tutorial presents a collection of query examples.

The DB access API supports two types of queries: `cone search` and `general search`.
We are using a `MongoDB` `NoQSL` database on the backend.
The query syntax might look unusual if you are experienced in `SQL`, but feels quite natural if you are using `python`. 
If you have a query in mind, but only know how to express it in English or in `SQL`, 
email that to Dima <a href="mailto:duev@caltech.edu">`duev@caltech.edu`</a>, you will get help 
for converting that into something understandable by the API.


#### Cone search

<span class="badge badge-success">Note</span> Use the cone search web interface to construct a query (click `Get query`) 
that can be used with `penquins`.

The user can run the cone search around an arbitrary number of sky positions. 
The (objects) coordinates are passed to the API as a string, but they must be written as a valid `python` expression,
namely as a list of tuples with `RA`'s and `Dec`'s in one of the three supported formats (see examples below):
- degrees (expressed as `float` or `int`)
- HH:MM:SS, DD:MM:SS (expressed as `string`) 
- HHhMMmSSs, DDdMMmSSs (expressed as `string`)

The cone search radius can be specified in `arcsec`, `arcmin`, `deg`, or `rad`.

When specifying which catalogs to search, use the `"filter"` key to define the constraints, and the
`"projection"` key to specify which catalog fields to retain in the query results (see examples below). 
By default, only the object catalog id and the coordinates are returned.

You can see all the catalog field names in the documentation. *todo* 

The optional `"kwargs"` key may be used to change the default behavior, and/or to store auxiliary data with the query 
(for example, for personal bookkeeping).

##### Examples 

*Sample cone search 1:*
Get all objects from the `PanSTARRS` catalog within `10` arcseconds from `('01h05m00.9631s', '-34d06m33.3505s')`
that were detected `N` times, where `1 < N < 100`, and return only the default fields. For the bookkeeping purposes,
specify the following key-value pair under `kwargs`: `"alert-id": "ZTF18omgalrt"`.

```python
q = {"query_type": "cone_search",
     "object_coordinates": {
         "radec": "[('01h05m00.9631s', '-34d06m33.3505s')]",
         "cone_search_radius": "10",
         "cone_search_unit": "arcsec"
     },
     "catalogs": {
         "PanSTARRS": {"filter": {"nDetections": {"$gt": 1, "$lt": 100}}, "projection": {}}
     },
     "kwargs": {
         "alert-id": "ZTF18omgalrt"}
     }
```

---

*Sample cone search 2*

Get all objects from the `ZTF_alerts` catalog within `8` arcseconds for two sky positions 
`(173.5155088, 33.0845502), (172.1345, 30.5412)`
and return, in addition to the default fields, `objectId`'s, `candidate.rcid`'s, and `candidate.rb`'s. 

```python
q = {"query_type": "cone_search",
     "object_coordinates": {
         "radec": "[(173.5155088, 33.0845502), (172.1345, 30.5412)]", 
         "cone_search_radius": "8",
         "cone_search_unit": "arcsec"
     },
     "catalogs": {
         "ZTF_alerts": {
             "filter": {},
             "projection": {
                 "objectId": 1,
                 "candidate.rcid": 1,
                 "candidate.rb": 1
             }
         }
     }
     }
```
<br>

#### General search

The general search interface allows the user to execute queries of arbitrary complexity.
The following `pymongo` operations are supported: 
`aggregate`, `map_reduce`, `distinct`, `count_documents`, `find_one`, `find`.

##### Recommendations

Kowalski gives a lot of power to the users, so it is expected that it is used responsibly.

<span class="badge badge-warning">Tip</span> It is always a good idea to test a query before running it "full steam" by
limiting the number of returned documents. For example, the following query will return at most 3 documents:

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'candidate.rb': {'$gt': 0.5}}).limit(3)" 
     }
``` 

Without this limitation, the output will be many TB in size.

<span class="badge badge-warning">Tip</span> If you are unsure about the size of the query result, you can try counting 
the number of returned documents first:

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].count_documents({'candidate.rb': {'$gt': 0.99}})" 
     }
```

<span class="badge badge-warning">Tip</span> Use projections to ditch the fields that you do not need in the result and
reduce its size:

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'candidate.rb': {'$gt': 0.99}}, {'candid': 1, 'objectId': 1, 'candidate.rb': 1}).limit(3)" 
     }
```

You can either specify which fields to return by saying `{'FIELD_1': 1, 'FIELD_2': 1, ...}`,
or which ones to leave out keeping all the rest: `{'FIELD_1': 0, 'FIELD_2': 0, ...}`.

<span class="badge badge-warning">Tip</span> You get the fastest execution time if the field(s) that you are querying
are indexed. In this case, the database needs to only perform a fast search in the index that in most cases fits
into Kowalski's memory. If there is no index available, the database will have to load all the actual documents into 
the memory, which may take a lot of time. You can check the available indexes like this:

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].index_information()" 
     }
```

Contact Dima if you need a field indexed.

<span class="badge badge-warning">Tip</span> When running aggregation pipelines, use `allowDiskUse=True` if you get the
"exceeded memory limit" error.

##### Examples

<span class="badge badge-success">Note</span> In the web interface, you should only type in the "de-stringed" 
`q['query']` value. You can think of it as of the result of `eval(q['query'])`.

*Find all ZTF alerts with a given objectId*

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'objectId': {'$eq': 'ZTF18aabcyiy'}})" 
     }
```

<span class="badge badge-success">Note</span> In the web interface, you should only type:

```python
db['ZTF_alerts'].find({'objectId': {'$eq': 'ZTF18aabcyiy'}})
```

---

*Find all ZTF alerts with given `objectId`'s*

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'objectId': {'$in': ['ZTF18aabcyiy', 'ZTF18aabexxf']}})" 
     }
```

---

*Find all distinct ZTF transient `objectId`'s*

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].distinct('objectId')" 
     }
```

---

*Get all ZTF transient `objectId`'s detected more than twice*

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].aggregate([{'$group' : { '_id': '$objectId', 'count': { '$sum': 1 } } }, {'$match': {'count' : {'$gt': 2} } }, {'$project': {'objectId' : '$_id', '_id' : 0} }], allowDiskUse=True)" 
    }
```

---

*Get all alert `objectId`'s for transients with more than 20 detections, each with an rb score of >= 0.5*

```python
q = { "query_type": "general_search", 
"query": "db['ZTF_alerts'].aggregate([{'$group' : { '_id': '$objectId', 
                                                    'count': { '$sum': {'$cond': [ { '$gt': [ '$candidate.rb', 0.5 ] }, 1, 0]} } } }, 
                                      {'$match': {'count' : {'$gt': 20} } }, 
                                      {'$project': {'objectId' : '$_id', '_id' : 0} }], allowDiskUse=True)" }
```

---

*Get all alert `objectId`'s for transients with more than 20 detections in R and i bands, each with an rb score of >= 0.5*

```python
q = { "query_type": "general_search", 
"query": "db['ZTF_alerts'].aggregate([{'$group' : { '_id': '$objectId', 
                                                    'count': { '$sum': { '$cond': [ { '$and': [ 
                                                                                                { '$in': [ '$candidate.fid', [2, 3] ] }, 
                                                                                                { '$gt': [ '$candidate.rb', 0.5 ] } 
                                                                                              ] 
                                                                                     }, 1, 0 ] 
                                                                        } 
                                                              } } }, 
                                      {'$match': {'count' : {'$gt': 20} } }, 
                                      {'$project': {'objectId' : '$_id', '_id' : 0} }], allowDiskUse=True)" }
```

---

*Get all alert `objectId`'s, `candidate.ra`'s, and `candidate.dec`'s for transients with more than 10 detections in g, R and i bands, each with an rb score of >= 0.5, within a box on the sky*

```python
q = { "query_type": "general_search", 
"query": "db['ZTF_alerts'].aggregate([
                                      {'$match': {'coordinates.radec_geojson': {'$geoWithin': { '$box': [[70.0 - 180.0, 48.0], [70.0005 - 180.0, 48.0005]] }}}},
                                      {'$group' : { '_id': {'objectId': '$objectId', 'ra': '$candidate.ra', 'dec': '$candidate.dec'},
                                                    'count': { '$sum': { '$cond': [ { '$and': [ { '$in': [ '$candidate.fid', [1, 2, 3] ] }, 
                                                                                                { '$gt': [ '$candidate.rb', 0.5 ] } 
                                                                                              ] 
                                                                                     }, 1, 0 ] 
                                                                        } 
                                                              } } }, 
                                      {'$match': {'count' : {'$gt': 10}} }, 
                                      {'$project': {'objectId' : '$_id.objectId', '_id' : 0, 'ra': '$_id.ra', 'dec': '$_id.dec'} }], allowDiskUse=True)" }
```

---

*Get time-stamped magnitude data for all detections of alert `ZTF18aaavrxt` with rb score of >= 0.5 and 
sort it by observation Julian date in descending order*

```python
q = { "query_type": "general_search", 
      "query": "db['ZTF_alerts'].find({'objectId': 'ZTF18aaavrxt', 'candidate.rb': {'$gt': 0.5}}, 
                                      {'candidate.jd': 1, 'candidate.magpsf': 1, 'candidate.rb': 1}).sort([('candidate.jd', DESCENDING)])" }
```

---

*Find all PanSTARRS objects with g-r<0, return source id's, colors, and positions*

```python
db['PanSTARRS'].find({'$expr': {'$lt': ['$gMeanPSFMag', '$rMeanPSFMag']}},
                     {'_id': 1, 'gMeanPSFMag': 1, 'rMeanPSFMag': 1, 'raMean': 1, 'decMean': 1})
```

---

*Get Gaia DR2 G-band light-curve for Gaia DR2 source with id 1796422062134294656*

Note that in the `Gaia_DR2_light_curves`, `source_id` field is `int64`.

```python
db['Gaia_DR2_light_curves'].find({'source_id': 1796422062134294656, 'band': 'G'}, {'_id': 0})
```

---

*Find all PanSTARRS objects with g-r < -0.1, return source id's, colors, and positions*

```python
db['PanSTARRS'].find({'gMeanPSFMag': {'$ne': -999}, 'rMeanPSFMag': {'$ne': -999}, '$expr': {'$gt': [{'$subtract': ['$gMeanPSFMag', '$rMeanPSFMag']}, -0.1]}},
                     {'_id': 1, 'gMeanPSFMag': 1, 'rMeanPSFMag': 1, 'raMean': 1, 'decMean': 1})
```

---

*Find all Gaia\_DR2 objects where the absolute G-band magnitude is below an empirical line in the HR diagram (that is, above a line when we plot the magnitude from low to high):
 M_G - 5 \* log10(1000 / parallax ) + 5  >  2.5\*(BP - RP) + 7.5, return source id's, phot\_g\_mean\_mag, bp-rp, and positions*

```python
db['Gaia_DR2'].find({'parallax': {'$gt': 0}, '$expr': {'$gt': [{'$add': [{'$subtract': ['$phot_g_mean_mag', {'$multiply': [5, {'$log10': {'$divide': [1000.0, '$parallax']}}]}]}, 5]}, {'$add': [{'$multiply': [2.5, '$bp_rp']}, 7.5]}]}},
                    {'_id': 1, 'phot_g_mean_mag': 1, 'bp_rp': 1, 'coordinates': 1})
```


##### Spatial (positional) queries

It is possible to use the general search interface to execute "spatial" queries, including (and not limited to)
cone, box, and polygon searches.

The `coordinates.radec_geojson` field defined for every object in the database (for all catalogs) has an
associated spherical 2D index, which allows for fast positional queries. `MongoDB` supports many
query operators, see [here](https://docs.mongodb.com/manual/reference/operator/query-geospatial/) 
for more details. The caveat to keep in mind is the following: `MongoDB` uses `GeoJSON` objects to represent `2D`
positions on the sphere. Both the longitude (RA) and latitude (Dec) must be expressed in decimal degrees, and the
valid longitude values are between `-180` and `180`, both inclusive, so you must subtract 180.0 degrees from your RA value.

---

*Find all ZTF alerts inside a box on the sky and return objectId, candidate.fid, coordinates.radec_str for each object found*

You must specify the bottom left and upper right coordinates of the box. 

```python
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].find({'coordinates.radec_geojson': {'$geoWithin': { '$box': [[70.0 - 180.0, 48.0], [70.0005 - 180.0, 48.0005]] }}}, {'objectId': 1, 'candidate.fid': 1, 'coordinates.radec_str': 1})"  
     }
```

*Run a complicated query, remove the 'cordinates' field from the result, and select a random sample from it*

```
q = {"query_type": "general_search", 
     "query": "db['ZTF_alerts'].aggregate([{'$match': {'candidate.jd': {'$gt': 2458322.500000}, 'candidate.isdiffpos': 't', 'candidate.programid': 1, 
                                                       'candidate.rb': {'$gt': 0.3, '$lt': 0.65}, '$expr': {'$gt': [{'$abs': '$candidate.ssdistnr'}, 8]}} }, 
                                           {'$project': {'coordinates': 0}}, {'$sample': { 'size': 100 }} ], allowDiskUse=True)"
     }

```

##### Misc

<a href="https://docs.mongodb.com/manual/reference/sql-aggregation-comparison/" target="_blank">SQL to MongoDB Aggregation Mapping Chart <i class="fa fa-external-link" aria-hidden="true"></i></a>