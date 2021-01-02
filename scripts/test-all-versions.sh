for python_version in "3.6" "3.7" "3.8"; do
    if [[ $python_version =~ $kPYTHON_VERSIONS ]] || [[ -z "$python_version" ]]; then
        bash scripts/test-version.sh "$python_version"
    else
        echo "Check python version"
    fi
done