---
title: Транзакции и exactly-once
sidebar_position: 6
description: "Публикация нескольких записей одной транзакцией Kafka, транзакции при пуле воркеров и обработчик «прочитать, преобразовать, записать» с exactly-once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Транзакции и exactly-once

Это руководство публикует несколько записей одной [транзакцией Kafka](../components/transactions/index.md), запускает транзакции при пуле воркеров и делает обработчик по схеме «прочитать, преобразовать, записать» exactly-once.

Транзакция Kafka делает набор записей видимым целиком.
Читатель с уровнем изоляции `read_committed`, который в librdkafka стоит по умолчанию, видит их все после фиксации транзакции и не видит ни одной после прерывания.
В RustStream транзакция - свойство политики публикации в точке монтирования, а обработчик называет только ту совместимость, которой пользуется.

## Всё или ничего из одного обработчика

`ship` превращает оплаченный заказ в отгрузочную строку на каждую позицию.
Заказ с пустой строкой отбрасывается, и ни одна его строка не должна попасть на склад.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#fanout"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#fanout"
```

</TabItem>
</Tabs>

Обработчик принимает свой слот как `Out<impl TransactionalPublisher, Shipments>`: он называет совместимость ядра [`TransactionalPublisher`](api:ruststream::TransactionalPublisher) и ничего из Kafka.
Записи, опубликованные между [`begin_transaction`](api:ruststream::TransactionalPublisher::begin_transaction) и [`commit`](api:ruststream::TransactionalPublisher::commit), становятся видимыми вместе.
[`abort`](api:ruststream::TransactionalPublisher::abort) отменяет их, поэтому первая строка заказа с пустой строкой так и не доходит до читателя.
Вне открытой транзакции слот публикует как обычный.
Второй `begin_transaction` при открытой транзакции сообщает [`KafkaError::TransactionBusy`](api:ruststream_rdkafka::KafkaError::TransactionBusy), а `commit` или `abort`, когда открытой транзакции нет, сообщает [`KafkaError::NoTransaction`](api:ruststream_rdkafka::KafkaError::NoTransaction).

Транзакция охватывает опубликованные строки, но не прочитанный заказ.
[`Commit::Tracked`](../components/settlement/tracked.md) фиксирует смещение заказа после того, как обработчик его подтвердил.
Если процесс упадёт между этими двумя шагами, заказ будет доставлен снова, и его строки уйдут во второй раз.
Этот разрыв закрывает [конвейер exactly-once](../components/transactions/exactly-once/index.md), о котором речь ниже.

## Транзакции при пуле воркеров

Один продюсер Kafka ведёт одну транзакцию за раз, поэтому обработчики пула воркеров не могут делить одного транзакционного издателя.
`invoice` запускает четыре партиции воркеров и даёт каждой исходной партиции собственную транзакцию.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#lanes"
```

</TabItem>
</Tabs>

`workers(4, by_key)` выбирает партицию воркеров по партиции записи, поэтому одна партиция обрабатывается по порядку на одной партиции воркеров.
Слот - [`PartitionLanes`](api:ruststream_rdkafka::PartitionLanes): [`for_partition`](api:ruststream_rdkafka::PartitionLanes::for_partition) возвращает транзакционного издателя партиции доставки и создаёт его при первом обращении.
Партиции воркеров, которые обрабатывают разные партиции, ведут свои транзакции одновременно.
Транзакционные идентификаторы следуют за партициями темы, а не за числом воркеров, поэтому изменение `workers(n)` не меняет ни одного идентификатора.

Этой схеме нужен [ключ партиционирования](../components/subscriptions/lane-key.md) по умолчанию.
[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) раскладывает одну партицию по нескольким партициям воркеров. Тогда они делили бы издателя одной партиции и сталкивались бы на его единственной транзакции.

## Конвейер exactly-once

`enrich` читает размещённый заказ и отвечает итогами, по которым планирует склад.
Каждый ответ публикуется ровно один раз, даже если сервис упадёт и заказ будет прочитан снова.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#eos"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#eos"
```

</TabItem>
</Tabs>

Обработчик только возвращает ответ, кода транзакций в нём нет.
[`Commit::Transactional("enrichment-1")`](api:ruststream_rdkafka::Commit::Transactional) запрещает подписке фиксировать смещения самой.
Вместо неё их фиксирует конвейер с тем же идентификатором, внутри транзакции, которая несёт ответы.
Поэтому прочитанная позиция и опубликованные записи сдвигаются вместе: фиксируются либо оба, либо ни одно.

## Точка монтирования

Точка монтирования называет политику за каждым слотом и ответом.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/exactly_once.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/exactly_once.rs#service"
```

</TabItem>
</Tabs>

[`transactional_id`](api:ruststream_rdkafka::KafkaPublish::transactional_id) превращает обычную политику в транзакционную.
Один транзакционный идентификатор принадлежит одному живому продюсеру: второй продюсер, запущенный с тем же идентификатором, отсекает первого, и тот больше не может фиксировать.
Дайте каждому запущенному экземпляру сервиса свой идентификатор, например по порядковому номеру пода.

[`per_partition`](../components/transactions/per-partition.md) делает идентификатор основой: издатели `invoice` получают идентификаторы `invoicing-1-p0`, `invoicing-1-p1` и так далее.

[`EosPublish::new("enrichment-1")`](api:ruststream_rdkafka::KafkaEosPublish::new) - это конвейер, и его идентификатор тот же, что называет режим фиксации подписки.
[`EosReplies`](../components/transactions/exactly-once/eos-replies.md) переносит исходную позицию каждой доставки на её ответ. По ней конвейер и сопоставляет одно с другим.
Без него первый ответ завершается ошибкой, а не уходит мимо транзакции.

## Окно фиксации

Конвейер собирает ответы в окно и фиксирует его каждые [`commit_interval`](api:ruststream_rdkafka::KafkaEosPublish::commit_interval) (по умолчанию 100 мс).
В конце окна он ждёт, пока завершится каждая доставка, которая в него публиковала, добавляет их позиции и метаданные группы в транзакцию и фиксирует её.
Поэтому ответ становится видимым не раньше чем через один интервал фиксации после того, как обработчик его вернул.

Неудачная публикация, неудачная фиксация или доставка, которая не завершилась вовремя, прерывают окно.
Тогда консьюмеры перематываются к последним зафиксированным смещениям, и всё окно обрабатывается заново в новой транзакции.
В зафиксированном выходе каждый ответ всё равно встречается один раз.

Конвейер ограничивает, что обработчики могут возвращать и как они читают:

- `retry()` из обработчика держит окно открытым до истечения срока транзакции, 30 секунд, если [`transaction_timeout`](api:ruststream_rdkafka::KafkaEosPublish::transaction_timeout) не задаёт другой, а затем окно читается заново.
- [`retry_after`](../components/settlement/retries.md) не место в обработчике конвейера: его отложенная копия публикуется вне окна и нарушает соответствие записей и смещений.
- [`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) не сочетается с `Commit::Transactional`, и такая подписка не запускается: конвейер фиксирует через группу консьюмеров, а читатель закреплённых партиций в группе не участвует.
