Bloom Filter Redis Module
=========================

Redis module that implements Bloom Filters using the redis module API. Needs Redis version >= 4.0.0.

### Install
```
make
```

### Load module to Redis
```
redis-server --loadmodule /path/to/bloom_filter.so
```

## Test
```
make test
```


Operations
----------

## `bf.init`

Initializes the Bloom Filter.
```
> bf.init key capacity error_rate seed
```
Last 3 parameters are optional.
- **capacity** is the number of expected different elements. Default is 1000000.
- **error_rate** the tolerated error_rate. Default is 0.01 (1%)
- **seed** initialization seed. Defaults to current time in microseconds.


Example:
```
> bf.init mykey 1000 0.05 1234
OK
```

## `bf.add`
Adds an element to the filter.
```
> bf.add key element
```
Example:
```
> bf.add mykey myelement
OK
```

## `bf.exists`
Says if an element exists in the filter.
```
bf.exists key element
```
Example:
```
> bf.exists mykey myelement
(integer) 1
> bf.exists mykey notmyelement
(integer) 0
```

## `bf.merge`
Merges key2 into key1.
```
bf.merge key1 key2
```
Keys must have same capacity, seed and error_rate. So they must have been initialized with all the parameters.

Example:
```
> bf.init myotherkey 1000 0.05 1234
OK
> bf.add myotherkey myotherelement
OK
> bf.merge mykey myotherkey
OK
> bf.exists mykey myotherelement
(integer) 1
```
