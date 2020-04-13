
##### simple multiprocessing implementation in python using pandas dataframe


```sh
git clone https://github.com/shemul/pandas-multiprocessing
cd pandas-multiprocessing
pipenv install
pipenv run python main.py --input_csv="./data/users.csv" --output_csv="./output/users.csv" --chunk_size=300 --pool=10
```
where `pool` indicates how many process will spawn and `chunk_size` defines how many rows will be process in every pool

#####  Todos
```inform7
- update readme.md
```
