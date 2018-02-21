# Id Index

This Dynamo table maps annotation ids to the cuboids they exist in.  Using
this table, it is easy to return a cutout that contains all the cuboids that
contain a given annotation id.

## Key

`channel-id-key` (S): key formed by the collection | experiment | channel | resolution | annotation id
`version` (N): range key (reserved for future use)

## Attributes

`cuboid-set` (SS): Set of all morton ids of the cuboids that the annotation id exists in
`lookup-key` (S): stores the collection | experiment | channel | resolution of the cuboid


## Global Secondary Indexes

### lookup-key-index

**This index is under consideration, but not yet created.**

Easily find all ids belonging to a particular collection, experiment, channel,
and resolution.

Potential use case: deleting ids belonging to a deleted channel.

##### Key

`lookup-key`

#### Projected Attributes

None
