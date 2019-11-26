# S3 Index

This Dynamo table tracks which cuboids exist in S3 cuboid bucket.  If the
cuboid belongs to an annotation channel, it also has a set that contains all
the annotation ids that exist inside the cuboid.

## Key

`object-key` (S): same key used in the S3 cuboid bucket
`version-node` (N): range key (reserved for future use)

## Attributes

`ingest-job-hash` (S): the collection id
`ingest-job-range` (S): the id of the ingest job that this cuboid was part of
`id-set` (SS): Set of all annotation ids that exist in the cuboid if this is part of an annotation channel
`lookup-key` (S): stores the collection | experiment | channel | resolution of the cuboid


## Global Secondary Indexes

### ingest-job-index

Find cuboids based on the ingest job.

#### Key

`ingest-job-hash`
`ingest-job-range` (range)

#### Projected Attributes

None


### lookup-key-index

Find all cuboids in a collection, experiment, channel at a resolution level.

##### Key

`lookup-key`

#### Projected Attributes

None
