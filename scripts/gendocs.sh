
rm -fr docs/api
pdoc --overwrite --html --html-dir docs/api ./pypeln

# while inotifywait -r -e modify,create,delete . ; do
#     rm -fr docs/api
#     pdoc --overwrite --html --html-dir docs/api ./pypeln
# done 