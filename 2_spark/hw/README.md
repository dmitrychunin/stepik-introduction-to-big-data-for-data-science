# Задание
В результате выполнения вашей задачи, например, выполнив команду:

`spark-submit PySparkJob.py clickstream.parquet result`
или
`python PySparkJob.py clickstream.parquet result`

Вы должны прочитать указанный в параметрах файл, обработать его и получить структуру папок вида:

    /result/train
    /result/test
    /result/validate

С наборами данных в следующем соотношении train/test/validate = 0.5/0.25/0.25 (randomSplit).
Где в каждой папке должен находиться parquet-файл (число партиций не принципиально) со следующей структурой данных:
| Имя колонки | Тип колонки | Описание |
| --- | --- | --- |
| ad_id | integer | id рекламного объявления |
| target_audience_count | decimal | размер аудитории, на которую таргетируется объявление |
| has_video | integer | 1 если есть видео, иначе 0 |
| is_cpm | integer | 1 если тип объявления CPM, иначе 0 |
| is_cpc | integer | 1 если тип объявления CPC, иначе 0 |
| ad_cost | double | стоимость объявления в рублях |
| day_count | integer | Число дней, которое показывалась реклама |
| CTR | double | Отношение числа кликов к числу просмотров |
