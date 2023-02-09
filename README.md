# Promantle
Experimental ways of storing data for high speed queries

* [x] Basic tooling and test jigs
* [ ] Custom range triangular data
* [ ] Multi-page data with baked-in sorting

## Requirements

Uses Cockroach DB: https://www.cockroachlabs.com/docs/releases/index.html

## Triangular data

A pre-aggregated data set for keeping a log of data, and being able to query arbitrary
ranges of data at different levels of detail.

* [x] Add ranks and aggregates
* [x] Query points and ranges
* [x] Get the upper/lower range of a single data point
* [x] Demonstrate averages
* [ ] Demonstrate different data types
* [x] Demonstrate different aggregates (max/min/..?)
* [ ] Tests around very sparse data

```

Rank Zero
(original
data pts)    Rank 1      Rank 2    . . .   Rank n
            :  ___      :
A     \     :     \     :
B      |    :      |    :
C      +---  AE    |    :
D      |    :      |    :
E     /     :      |___  AJ               ↑
F  \        :      |    :                 :
G   |       :      |    :                 :
H   +------  FJ    |    :                 : AN
I   |       :      |    :                 :
J  /        :  ___/     :                 :
K    \      :       \   :                 ↓
L     |____  KN      |__ KN
M     |     :        |  :
N    /      :       /   :

```