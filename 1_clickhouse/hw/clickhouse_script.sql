--№1 Получить статистику по дням. Просто посчитать число всех событий по дням, число показов, число кликов, число уникальных объявлений и уникальных кампаний.
--SQL:
SELECT date,
       count(*)                 all_event_count,
       countIf(event = 'view')  view_event_count,
       countIf(event = 'click') click_event_count,
       uniqExact(ad_id)              unique_ad_count,
       uniqExact(campaign_union_id)  unique_campaign_count
FROM ads_data
group by date;

--№2 Разобраться, почему случился такой скачок 2019-04-05? Каких событий стало больше? У всех объявлений или только у некоторых?
--SQL:
select ad_id, date, countIf(event = 'view') view, countIf(event = 'click') click
from ads_data
group by ad_id, date
order by view desc, click desc
limit 10;
--Вывод:
-- 2019-04-05 было выявлено 3 "аномальных" объявления с огромным числом событий: 112583, 107729 и 28142
-- Преимущественно стало больше событий показа, но количество кликов также выросло, за счет объявления 112583
-- 2019-04-05 в объявлении 112583 события view: 302811, click: 91017
-- 2019-04-05 в объявления 107729 и 28142 в сумме имеют 50к показов

--№3 Найти топ 10 объявлений по CTR за все время. CTR — это отношение всех кликов объявлений к просмотрам. Например, если у объявления было 100 показов и 2 клика, CTR = 0.02. Различается ли средний и медианный CTR объявлений в наших данных?
--SQL:
select ad_id,
       round(countIf(event = 'click') / countIf(event = 'view'), 3) ctr
FROM ads_data
group by ad_id
having countIf(event = 'click') <= countIf(event = 'view')
   and countIf(event = 'view') > 0
order by ctr desc
limit 10;

--SQL (первый вариант расчета CTR через массив):
select round(avg(ctr), 5) average, round(median(ctr), 5) median
from (
      select if (has(groupArray(event), 'view'),
                 countEqual(groupArray(event), 'click') / countEqual(groupArray(event), 'view'),
                 0) ctr
      FROM ads_data
      group by ad_id
         );
--SQL (второй вариант расчета CTR через комбинаторы агрегатных функций):
select round(avg(ctr), 5) average, round(median(ctr), 5) median
from (
      select countIf(event = 'click') / countIf(event = 'view') ctr
      FROM ads_data
      group by ad_id
      having countIf(event = 'click') <= countIf(event = 'view') and countIf(event = 'view') > 0
         );
--Вывод:
--средний (0.01584) и медианный (0.00286) CTR объявлений различается

--№4 Похоже, в наших логах есть баг, объявления приходят с кликами, но без показов! Сколько таких объявлений, есть ли какие-то закономерности? Эта проблема наблюдается на всех платформах?
--SQL:
select count(*)
from (
      select ad_id
      FROM ads_data
      group by ad_id
      having countIf(event = 'view') < countIf(event = 'click')
         );
--Вывод:
--Всего 9 таких объявлений

--SQL:
select ad_id,
       toDate(time) date,
       platform,
       countIf(event = 'view') - countIf(event = 'click')
FROM ads_data
group by ad_id, toDate(time), platform
having countIf(event = 'view') < countIf(event = 'click')
order by ad_id;
--Вывод:
-- дата date != дате time
-- сбои произошли только 2019-04-01, если ориентироваться на дату time
-- проблема наблюдается на всех платформах (android, ios, web)

-- РЕЦЕНЗИЯ ПРЕПОДАВАТЕЛЯ:
-- 4. Стоит также выяснить, чем знаменательны проблемные объявления. Может быть у них у всех один тип оплаты или у всех есть видео?

-- SQL:
select ad_id,
       ad_cost_type
FROM ads_data
group by ad_id, ad_cost_type
having countIf(event = 'view') < countIf(event = 'click')
order by ad_id;
-- Вывод:
-- Тип оплаты у подобных объявлений встречается как CPM так и CPC, но преобладает CPM

-- SQL:
select ad_id,
       has_video
FROM ads_data
group by ad_id, has_video
having countIf(event = 'view') < countIf(event = 'click')
order by ad_id;
-- Вывод:
-- У всех объявлений отсутствует видео

-- По полям campaign_union_id, ad_cost, target_audience_count аналогичными запросами закономерностей не было найдено


--№5 Есть ли различия в CTR у объявлений с видео и без? А чему равняется 95 процентиль CTR по всем объявлениям за 2019-04-04?
--SQL:
select medianIf(ctr, has_video = 0) median_without_video,
       avgIf(ctr, has_video = 0)           avg_without_video,
       medianIf(ctr, has_video = 1) median_with_video,
       avgIf(ctr, has_video = 1)           avg_with_video
FROM (
      select countIf(event = 'click') / countIf(event = 'view') ctr,
             has_video
      FROM ads_data
      group by ad_id, has_video
      having countIf(event = 'view') >= countIf(event = 'click')
         );
--Вывод:
-- среднее значение и медиана CTR у объявлений с видео выше, по сравнению с объявлениями без видео -> по объвлениям с видео в среднем кликают чаще

--SQL:
select round(quantile(0.95)(ctr), 3)
FROM (
      select countIf(event = 'click') / countIf(event = 'view') ctr
      FROM ads_data
      where date = toDate('2019-04-04')
      group by ad_id
      having countIf(event = 'view') >= countIf(event = 'click')
         );
--Вывод:
-- 95 процентиль CTR по всем объявлениям за 2019-04-04 равно 0.082

--№6 Для финансового отчета нужно рассчитать наш заработок по дням. В какой день мы заработали больше всего? В какой меньше? Мы списываем с клиентов деньги, если произошел клик по CPC объявлению, и мы списываем деньги за каждый показ CPM объявления, если у CPM объявления цена - 200 рублей, то за один показ мы зарабатываем 200 / 1000.
--SQL:
select date,
       round(sumIf(ad_cost, ad_cost_type = 'CPC' and event = 'click') +
             sumIf(ad_cost, ad_cost_type = 'CPM' and event = 'view') / 1000, 2) proceed
from ads_data
group by date
order by proceed desc;
--Вывод:
--Больше всего заработали 2019-04-05 - 96123.12, меньше всего 2019-04-01 - 6655.71

--№7 Какая платформа самая популярная для размещения рекламных объявлений? Сколько процентов показов приходится на каждую из платформ (колонка platform)?
--SQL:
select platform,
       countIf(event = 'view')  view_count,
       countIf(event = 'click') click_count
from ads_data
group by platform
order by view_count desc, click_count desc;
--Вывод:
-- Самая популярная для размещения платформа - android

--SQL:
select round(100 * countIf(platform = 'ios' and event = 'view') / countIf(event = 'view'), 2)     ios,
       round(100 * countIf(platform = 'web' and event = 'view') / countIf(event = 'view'), 2)     web,
       round(100 * countIf(platform = 'android' and event = 'view') / countIf(event = 'view'), 2) android
from ads_data;
--Вывод:
-- Проценты показа по каждой платформе ios: 29.99%, web: 19.98%, android: 50.03%

--№8 А есть ли такие объявления, по которым сначала произошел клик, а только потом показ?
--SQL:
select ad_id
from ads_data
group by ad_id
having minIf(time, event = 'click') < minIf(time, event = 'view')
   and minIf(time, event = 'click') > 0
order by ad_id;
--Вывод:
-- Такие объвления есть, всего их 12, список ad_id: [46639 ,38224 ,107798 ,33033 ,44766 ,36758 ,44283 ,114886 ,23599 ,98569 ,32386 ,18681]

-- РЕЦЕНЗИЯ ПРЕПОДАВАТЕЛЯ:
-- 8. Более простой вариант - найти все объявления, у которых самым первым событием был клик.

-- РАБОТА НАД ОШИБКАМИ:
-- Попробовал решить другим вариантом, т.к. не знал как упростить предыдущее значение:
-- создаем массив кортежей (time, event), группируя по объявлениям
-- сортируем по времени
-- берем первый элемент массива (самое первое событие)
-- сравниваем тип события с 'click'
select ad_id
from ads_data
group by ad_id
having arrayElement(arraySort((x) -> x.1 ,groupArray(tuple(time, event))), 1).2 = 'click'
order by ad_id;
-- оказалось что таких событий 21
-- Вывод: в предыдущем запросе не были учтены null значения агрегата minIf (когда не было событий по показам)