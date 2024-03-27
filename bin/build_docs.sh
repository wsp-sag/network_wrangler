# Exit on error
set -e
echo "Building docs"
cd docs

sphinx-build -b html source build

cd _build
touch .nojekyll
