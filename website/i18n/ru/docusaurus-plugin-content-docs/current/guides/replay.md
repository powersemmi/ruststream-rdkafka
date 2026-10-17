---
title: Повторное чтение и перемотка
sidebar_position: 5
description: "Повторное чтение темы с начала или с заданного момента и перемотка работающей подписки из её обработчика."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Повторное чтение и перемотка

Это руководство читает тему заново с начала или с заданного момента и перематывает работающую подписку из её обработчика.

Kafka хранит журнал и после того, как консьюмер его прочитал.
Сервис может прочитать его снова: чтобы перестроить модель чтения, заполнить данными новый консьюмер или пропустить записи, которые ему не годятся.
Подписка получает [начальную позицию](../components/positions/start-at.md) в точке монтирования, а обработчик перематывает свою подписку через [контекст](../components/context/index.md) доставки.

## Модели чтения

Сервис держит в памяти две модели чтения: баланс каждого счёта и расходы по каждой карте.
Каждая проводка несёт ревизию реестра.
Исправление прошлых проводок публикуется новой ревизией, и модель чтения, которая её встречает, перестраивается с начала журнала.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#projection"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#projection"
```

</TabItem>
</Tabs>

## Чтение с начала

Балансы живут в памяти, поэтому после перезапуска они строятся заново по всему журналу.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#balance"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#balance"
```

</TabItem>
</Tabs>

`start_at(KafkaPosition::earliest())` открывает подписку с начала журнала при каждом запуске, что бы группа `balances` ни зафиксировала раньше.
[`StartOffset`](../components/subscriptions/start-offset.md) в дескрипторе работает иначе: он действует, только пока у группы нет зафиксированного смещения.
Позиция ставится на партиции, которые группа назначает этому консьюмеру, до их первой записи.

У [`KafkaPosition`](../components/positions/index.md) пять позиций:

| Позиция | Откуда продолжает подписка |
|---|---|
| [`earliest()`](api:ruststream_rdkafka::KafkaPosition::earliest) | с самой старой записи, которую хранит Kafka |
| [`latest()`](api:ruststream_rdkafka::KafkaPosition::latest) | с конца журнала, поэтому читаются только новые записи |
| [`offset(partition, n)`](api:ruststream_rdkafka::KafkaPosition::offset) | со смещения `n` одной партиции |
| [`topic_offset(topic, partition, n)`](api:ruststream_rdkafka::KafkaPosition::topic_offset) | со смещения `n` одной партиции одной темы, для подписки на несколько тем |
| [`timestamp(ms)`](api:ruststream_rdkafka::KafkaPosition::timestamp) | с первой записи с отметкой времени не раньше `ms`, в каждой партиции |

Если начальная позиция не называет ни одной партиции подписки, запуск завершается ошибкой.
При [`Assignment::CooperativeSticky`](../components/subscriptions/assignment.md) первое назначение может быть лишь частью доли группы, а партиции, которые приходят следующими порциями, не перематываются.

## Перемотка из обработчика

Тот же обработчик перестраивает балансы, когда приходит проводка более новой ревизии.
[`Ctx<SeekHandle>`](../components/positions/seek.md) даёт обработчику [`KafkaSeeker`](api:ruststream_rdkafka::KafkaSeeker) его собственной подписки.
[`seek`](api:ruststream::Seeker::seek) перематывает подписку на начало журнала, и обработчик читает все проводки заново.
Проводка, с которой началась перестройка, тоже возвращается, и на этот раз проекция её применяет.

Перемотка принимает любую из позиций выше.
`Ctx<Position>` хранит собственную позицию доставки: перемотка на неё читает запись заново, а за ней и всё последующее.
`Ctx<Partition>` и `Ctx<Offset>` дают координаты для позиции `offset(..)`.

## Перемотка из пакетного обработчика

Проводки по картам идут потоком, поэтому расходы строятся [пакетами](../components/subscriptions/batches.md) до 500 штук.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#batch"
```

</TabItem>
</Tabs>

Пакетный обработчик объявляет [`KafkaBatchContext`](../components/context/batch-context.md) вместо контекста одной доставки.
В нём тот же ключ `SeekHandle` и нет позиции, потому что пакет охватывает много записей.
Подписка перематывается, когда завершён весь пакет.

## Чтение с заданного момента

Проверка выплат при каждом запуске читает выплаты за последние семь дней.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#payouts"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#payouts"
```

</TabItem>
</Tabs>

Момент известен только при запуске сервиса, поэтому позицию задаёт точка монтирования, а не атрибут.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/replay.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/replay.rs#service"
```

</TabItem>
</Tabs>

`KafkaPosition::timestamp` принимает миллисекунды от начала эпохи: в этих единицах Kafka ставит отметки времени на записи.
Kafka находит момент в каждой партиции по первой записи с отметкой не раньше него.
Партиция без такой записи продолжает со своего конца.

## Что перемотка делает с группой

Перемотка двигает этот консьюмер, а не группу.
Она переносит партиции, которые держит этот участник, и ни за кого ничего не фиксирует.
Остальные участники группы продолжают читать с того места, где находятся.
Следующая перебалансировка отменяет перемотку: тот, кому достанется партиция, продолжит с зафиксированного смещения группы.
Позиция `start_at` применяется заново при каждом запуске.

## Перемотка при Commit::Tracked

Все три подписки фиксируют смещения с [`Commit::Tracked`](../components/settlement/tracked.md) (см. [Фиксация и гарантии доставки](commits.md)).
Перемотка сбрасывает отслеживаемую позицию каждой партиции, которую переносит.
Доставка, полученная до перемотки, завершается впустую, поэтому подтверждение проводки, с которой началась перестройка, ничего не фиксирует.
Так следующая фиксация никогда не обгоняет запись, которую перемотка прочитала заново и которую ещё не обработал ни один обработчик.
