---
title: Темы, группы консьюмеров и порядок
sidebar_position: 2
description: "Чтение темы в группах консьюмеров, порядок каждого ключа при параллельной обработке и размещение записей по партициям."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Темы, группы консьюмеров и порядок

Это руководство читает тему в группах консьюмеров, сохраняет порядок каждого ключа на параллельных партициях воркеров и размещает записи по партициям: по ключу, по номеру или по очереди.

Пример - сервис банковского реестра.
Он проводит переводы по счетам, проверяет их на мошенничество, ведёт указатель своих записей и открывает дела по оспоренным переводам.

## Порядок по счёту

Kafka сохраняет порядок записей внутри одной партиции.
Записи с одинаковым ключом попадают в одну партицию, поэтому порядок каждого ключа сохраняется.
`post` читает переводы с ключом по счёту и обрабатывает до восьми одновременно, не нарушая этого порядка.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#post"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#post"
```

</TabItem>
</Tabs>

`workers(8, by_key)` запускает восемь партиций воркеров. Доставки с одинаковым [ключом партиционирования](../components/subscriptions/lane-key.md) выполняются друг за другом на одной партиции воркеров.
[`LaneKey::RecordKey`](api:ruststream_rdkafka::LaneKey::RecordKey) выбирает партицию воркеров по ключу записи, поэтому два перевода одного счёта никогда не выполняются одновременно, даже внутри одной партиции.
Значение по умолчанию, [`LaneKey::Partition`](api:ruststream_rdkafka::LaneKey::Partition), выбирает её по исходной партиции. Так обрабатываются и записи без ключа.
В ручной форме та же настройка - [`workers_by_key`](api:ruststream::runtime::SubscriberSettings::workers_by_key).

Ключ записи приходит в обработчик заголовком `kafka-partition-key` ([`PARTITION_KEY_HEADER`](../components/publishing/keys.md)).
`AccountKey` - контракт заголовков с одним полем под этим именем.
`Headers<AccountKey>` читает ключ доставки, а `with_headers(&key)` записывает его ключом проводки.
Поэтому и в `bank.entries` проводка попадает в партицию своего счёта, и читатели реестра видят каждый счёт по порядку.
Перевод без ключа не проходит контракт заголовков, и его завершает политика декодирования, которая по умолчанию его отбрасывает.

## Группы консьюмеров

Группа консьюмеров читает каждую запись один раз, какому бы участнику ни досталась партиция.
Каждая группа читает свою копию.
`screen` читает те же переводы в собственной группе, поэтому видит каждый перевод, который видит `post`.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#screen"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#screen"
```

</TabItem>
</Tabs>

[`KafkaTopic::group`](../components/subscriptions/kafka-topic.md) задаёт группу.
Подписка, которая не называет группу, входит в [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group) брокера. Так делает `post`.
Kafka читает тему только в составе группы, поэтому сервис, у которого нет ни того, ни другого, не запускается.

[`StartOffset`](../components/subscriptions/start-offset.md) говорит, откуда начинает группа, у которой нет зафиксированного смещения для партиции.
Так бывает при первом запуске и после того, как срок хранения удалил её смещение.
`Earliest` начинает с самой старой записи, `Latest` - с конца журнала, а `Committed`, значение по умолчанию, оставляет выбор свойству librdkafka `auto.offset.reset`, которое равно `latest`.

[`Assignment`](../components/subscriptions/assignment.md) выбирает, как группа делит партиции между участниками.
`CooperativeSticky` при входе или выходе участника переносит только партиции, которые меняют владельца, а остальные продолжают работать.

Запуск ждёт, пока группа назначит этому участнику его партиции, но не дольше [`assignment_timeout`](../components/broker/startup.md) (по умолчанию 60 секунд).
После этого каждая запись, опубликованная в тему, доходит до подписки.
Дайте каждой подписке сервиса свою группу.
Две подписки на разные темы в одной группе вызывают перебалансировку друг у друга: когда входит вторая, первая на миг теряет свои партиции.
Запись, опубликованная в этот миг, читается оттуда, куда `auto.offset.reset` вернёт первого участника, а при `latest` это место уже после записи.

## Несколько тем и шаблон

[`KafkaTopics`](../components/subscriptions/kafka-topics.md) читает несколько тем через один консьюмер и одну группу.
[`KafkaTopics::pattern`](../components/subscriptions/patterns.md) читает все темы, имя которых подходит под регулярное выражение. Выражение должно начинаться с `^`.
`archive` читает все темы банка и запоминает, где лежит каждая запись.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#archive"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#archive"
```

</TabItem>
</Tabs>

Обработчик берёт координаты своей доставки из [контекста](../components/context/index.md): [`Topic`](api:ruststream_rdkafka::context::keys::Topic), [`Partition`](api:ruststream_rdkafka::context::keys::Partition) и [`Offset`](api:ruststream_rdkafka::context::keys::Offset).
У всех подходящих тем один тип полезной нагрузки. `archive` декодирует любой JSON.
Тема, созданная после запуска, входит в шаблон при следующем обновлении метаданных librdkafka.

[`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) читает названные партиции и не входит ни в какую группу.
Перебалансировки у неё не бывает. Это подходит для одного процесса на партицию или для инструмента, который изучает одну партицию.

## Размещение записей по партициям

`open_case` превращает оспоренный перевод в дело в теме `bank.cases`.
У этой темы три партиции: 0 и 1 поровну делят обычные дела, а партиция 2 хранит срочные.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#cases"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#cases"
```

</TabItem>
</Tabs>

[`partition(n)`](../components/publishing/partition.md) закрепляет одну запись за партицией.
Это шаг [`KafkaPublishSteps`](api:ruststream_rdkafka::KafkaPublishSteps), поэтому слот ограничен `Publisher<Options = KafkaOptions>`, чтобы этот шаг был доступен.
Закреплённая партиция важнее ключа записи, а ключ записи важнее партиционера librdkafka.
Публикация в партицию, которой у темы нет, завершается ошибкой.

## Приложение

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/groups_and_ordering.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/groups_and_ordering.rs#app"
```

</TabItem>
</Tabs>

[`RoundRobin::partitions(2)`](../components/publishing/round-robin.md) кладёт каждое дело, которое не называет партицию, в следующую из партиций 0 и 1.
Запись без ключа librdkafka кладёт в случайную партицию, и одни консьюмеры оказываются загружены сильнее других. Цикл распределяет записи поровну.
Запись с ключом или с закреплённой партицией сохраняет своё размещение.
Число партиций в цикле фиксировано: оно равно числу партиций, которые цикл должен использовать, и не больше, чем есть у темы.

У подписки на набор тем нет одной темы, куда отправить копию для повтора, поэтому приложение называет её через [`ToSourceTopic`](../components/settlement/to-source-topic.md). Это объясняет руководство [Повторы и темы dead-letter](retries.md).

Тесты в файле примера запускают приложение внутри процесса.
[Kafka внутри процесса](../components/testing/index.md) даёт каждой теме одну партицию, поэтому тест размещения по нескольким партициям работает с кластером:

```bash
KAFKA_TEST_URL=localhost:9092 cargo test
```
