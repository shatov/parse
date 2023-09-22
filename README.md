# parse
Парсинг почтового лога и поиск сообщений по адресу получателя

1. Для начала необходимо создать БД MySQL (Mariadb) см. [create_db](https://github.com/shatov/parse/blob/main/create_db)

2. Для запуска парсинга (входной файл out)
```
perl parse.pl
```

3. Для запуска веб-интерфейса
```
perl web.pl
```

При использовании существующей БД необходимо изменить значения переменных $host, $dbase, $user, $password в [parse.pl](https://github.com/shatov/parse/blob/main/parse.pl) и [web.pl](https://github.com/shatov/parse/blob/main/web.pl)
