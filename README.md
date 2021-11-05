# Threshold Queries

This repository contains scripts for experiments on threshold queries.

## Requirements

* [Python 3+](https://www.python.org/) (the recommended version is 3.8 or higher)
* [NetworkX](https://networkx.org/) Python package
* [PostgreSQL 12.8+](https://www.postgresql.org/) (the recommended version is 13.4)

## Configuration

### Data information
* kind of data [barabasi albert (`ba`), `imdb`, `full`]
* number of nodes (`n`)          [input param for ba, full]
* out degree      (`m0`)         [input param for ba] 
* imdb link types (`link-types`) [optional input param for imdb]
* number of edges (`m`)          [calculated for ba, imdb, full]
* indexing        (`indexed`)    [input param for ba, imdb, full]

```
data : {
   'kind'      : {'ba', 'full', 'imdb'}
   'n'         : int, 
   'm0'        : int,
   'link-types : int+, 
   'm'         : int,
}
```

### Query specification

* query class (`TQ1`, `TQ2`, `TQ3`)
* query method (`naive` or `windowed`)
* number of joins (`k`)
* threshold value 

```
query = {
   'class'     : {'TQ1','TQ2','TQ3'}
   'method'    : {'naive','windowed'}
   'k'         : int
   'threshold' : int 
}
```

## Data

The `movie_link.csv` file is from a 2013 IMDB snapshot used in the paper "How Good Are Query Optimizers, Really?" by Viktor Leis, Andrey Gubichev, Atans Mirchev, Peter Boncz, Alfons Kemper, Thomas Neumann (PVLDB Volume 9, No. 3, 2015).

The `movie_link.csv` file can be found in `raw-data`.

## Contribution

Would you like to improve this project? Great! We are waiting for your help and suggestions. If you are new in open source contributions, read [How to Contribute to Open Source](https://opensource.guide/how-to-contribute/).

## License

Distributed under [MIT License](https://github.com/domel/TQs/blob/main/LICENSE).
