Тестирование проводилось на наборе данных, содержащем связи между пользователями, фильмами, лайками и закладками, с общим количеством записей, равным 10 000 000.

Для MongoDB были построены индексы, так как без них тестирование не могло бы дать объективных результатов.  
Все операции успешно отображаются без задержек, что подтверждается результатами теста на реальное время обработки данных, где не было зафиксировано проблем с моментальным обновлением данных.

Время на вставку данных в MongoDB составило 261.62 секунд, что значительно быстрее, чем в PostgreSQL, где вставка данных заняла 2818.10 секунды. Таким образом, MongoDB продемонстрировал скорость вставки данных, которая была выше, чем у PostgreSQL, более чем в 10 раз.

Скорость чтения данных из PostgreSQL была выше для: "Список понравившихся фильмов", "Список закладок".
Однако PostgreSQL потребовалось значительно больше времени для: "Количество лайков у фильма", "Средняя оценка фильма".
Этой проблемы можно было бы избежать за счет индексирования, однако скорость вставки данных заметно бы снизилась, а ведь она и так не поспевает за Mongo.

| Запрос                                      | **PostgreSQL**    | **MongoDB**       |
|---------------------------------------------|-------------------|-------------------|
| **Список понравившихся фильмов**            | 0.0008 секунд     | 0.0016 секунд     |
| **Количество лайков у фильма**              | 0.1808 секунд     | 0.0013 секунд     |
| **Список закладок**                         | 0.0005 секунд     | 0.0012 секунд     |
| **Средняя оценка фильма**                   | 0.1476 секунд     | 0.0013 секунд     |

Мы выбрали MongoDB из-за его высокой скорости вставки данных, гибкости в структуре и возможности масштабирования, что идеально подходит для работы с большими объемами динамичных данных.
