### Для запуска:
1. Требуется наличие установленного docker и docker-compose
2. Выполнить сборку ```docker build --rm -t puckel/docker-airflow .```
3. Запустить 
```docker-compose up -d```
4. Добавить Connection для ClickHouse с connection_id=dwh-ch, connection_type=http
5. Добавить Connection для Postgres с connection_id=hs_pg
6. Убедившись в доступности источника и приёмника данных, включить DAG

(за основу взят образ https://github.com/puckel/docker-airflow)
