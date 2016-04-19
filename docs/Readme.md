# Generating HTML Documentation

`$SPDB` is the location of the spdb repository.

```shell
cd $SPDB/docs/SphinxDocs

# Ensure Sphinx and the ReadTheDocs theme is available.
pip3 install -r requirements.txt

./makedocs.sh
```

Documentation will be placed in `$SPDB/docs/SphinxDocs/_build/html`.
