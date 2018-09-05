
rm -fr docs
pdoc --overwrite --html --html-dir docs ./pypeln

while inotifywait -r -e modify,create,delete . ; do
    rm -fr docs
    pdoc --overwrite --html --html-dir docs ./pypeln
done 