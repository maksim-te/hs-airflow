### Для запуска:
1. Требуется наличие установленного docker и docker-compose
2. Запустить 
```docker-compose up -d```
3. Добавить Connection для ClickHouse с connection_id=dwh-ch, connection_type=http
4. Добавить Connection для Postgres с connection_id=hs_pg
5. Убедившись в доступности источников, включить DAG

(за основу взят образ https://github.com/puckel/docker-airflow)
