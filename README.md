# gen-tp-insert-sql

> 1. 支持从某个mysql数据库中导出查询结果并生成插入到另一个数据库中的语句，同时支持导出查询结果保存到csv文件中(可选)，保存插入语句到sql文件中
> 2. 支持从csv文件中生成插入语句，并保存到sql文件中，由于csv中不知道各个字段的类型，需要进行指定

```
python3 -m venv venv

cd venv

source bin/activite

cd ..

pip install -r requirements.txt

cp config.example.yml config.yml

python main.py gen-from-db

python main.py gen-from-csv
```
