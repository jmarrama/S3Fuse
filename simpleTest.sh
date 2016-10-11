mkdir -p data/buried/deep
echo hello, world >data/greeting
echo gold >data/buried/deep/loot
( cd data && zip -r -q ../archive.zip . )
rm -rf data
