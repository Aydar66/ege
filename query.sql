-- 1) from_to
--    - для каждой записи в таблице chat_messages:
--      a) преобразуем unix-время (created_at) в тип timestamp (sent_at);
--      b) с помощью оконных функций lag(...) определяем:
--         - кто отправлял предыдущее сообщение в этом же диалоге (received_from),
--         - когда было отправлено предыдущее сообщение (received_at).
--    - group by (partition by) entity_id => для каждого диалога (сделки),
--      order by created_at => чтобы определить хронологию сообщений.

with from_to as (
  select
    entity_id as deal,                         -- идентификатор диалога (сделки)
    created_by as sender,                      -- кто отправил текущее сообщение
    to_timestamp(created_at) as sent_at,       -- дата и время отправки (из unix)
    lag(created_by) over(partition by entity_id order by created_at) as received_from,   -- отправитель предыдущего сообщения
    lag(to_timestamp(created_at)) over(partition by entity_id order by created_at) as received_at  -- время предыдущего сообщения
  from chat_messages cm
),
-- 2) answers_to_clients
--    - берём из from_to только сообщения, которые:
--      a) не идут подряд от одного и того же отправителя (received_from != sender)
--      b) и при этом отправлены менеджером (sender != 0).
--    - Таким образом, здесь остаются "ответы менеджеров на клиентские сообщения".
answers_to_clients as (
  select
    *
  from from_to
  where received_from != sender
    and sender != 0
),
-- 3) corrected_night_dates
--    - Корректируем (сдвигаем) "ночное" время сообщений к 09:30 того же дня.
--    - "Ночными" считаем сообщения с временем между 00:00:00 и 09:30:00.
--    - Если sent_at (время отправки менеджером) попадает в этот интервал, то заменяем его на 09:30 (этого же дня),
--      аналогично для received_at (время клиентского сообщения).
--    - В остальных случаях оставляем время без изменений.
corrected_night_dates as (
  select
    sender,
    case 
      when sent_at::time > '00:00:00' and sent_at::time < '09:30:00'
        then date_trunc('day', sent_at) + interval '9 hour 30 minute'
      else sent_at
    end as sent_at,
    case 
      when received_at::time > '00:00:00' and received_at::time < '09:30:00'
        then date_trunc('day', received_at) + interval '9 hour 30 minute'
      else received_at
    end as received_at
  from answers_to_clients
),
-- 4) time_diff
--    - Считаем, сколько минут прошло между предыдущим (received_at) и текущим (sent_at) сообщениями.
--    - Если оба сообщения в пределах одного и того же календарного дня (received_at::date = sent_at::date),
--      то берём простую разницу (extract(epoch from sent_at - received_at) / 60).
--    - Если сообщения пришли в разные дни, то вычисляем:
--         (23:59:59 - время предыдущего сообщения) + (время текущего сообщения - 09:30:00).
--      Это нужно для учёта, что нерабочее время (с 00:00:00 до 09:30:00) "пропускается",
--      и фактически менеджер может ответить только после 09:30.
time_diff as (
  select
    sender,
    case
      when received_at::date = sent_at::date
        then extract(epoch from sent_at - received_at) / 60
      else extract(epoch from ('23:59:59' - received_at::time) + (sent_at::time - '09:30:00')) / 60
    end as minutes_diff
  from corrected_night_dates
),
-- 5) final_table
--    - Группируем по каждому менеджеру (sender) и берём среднее (avg) времени ответа (minutes_diff).
--    - Округляем до 1 десятичного знака (round(...,1)).
--    - Сортируем по среднему времени ответа (order by 2).
final_table as (
  select
    sender,
    round(avg(minutes_diff),1) as avg_minutes
  from time_diff
  group by 1
  order by 2
)
-- Итоговый SELECT:
--   - Джойним (final_table) с таблицами managers и rops,
--     чтобы получить имена менеджеров и их руководителей (boss).
--   - Выводим: rop_name (boss), name_mop (manager), и среднее время ответа (avg_minutes).
--   - Сортируем по avg_minutes.
select
  r.rop_name as boss,
  m.name_mop as manager,
  f.avg_minutes
from final_table f
join managers m
  on m.mop_id::int = f.sender
join rops r
  on r.rop_id = m.rop_id::int
order by 3