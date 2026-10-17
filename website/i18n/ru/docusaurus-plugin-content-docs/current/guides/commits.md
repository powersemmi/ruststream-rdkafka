---
title: Фиксация и гарантии доставки
sidebar_position: 3
description: "Что фиксируют ack и nack в Kafka: собственная фиксация librdkafka, точное at-least-once с Commit::Tracked и пакеты, завершаемые поэлементно."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Фиксация и гарантии доставки

Это руководство выбирает, что фиксируют `ack` и `nack` в Kafka: собственную фиксацию librdkafka, точное at-least-once с [`Commit::Tracked`](../components/settlement/tracked.md) или пакеты, которые завершаются поэлементно.

Kafka не завершает отдельные сообщения.
Группа консьюмеров фиксирует позицию в каждой партиции, и всё, что ниже неё, считается прочитанным.
Обработчик всё равно завершает каждую доставку, поэтому режим [`Commit`](../components/settlement/index.md) подписки решает, как эти завершения превращаются в позицию.

| Режим | `ack` | `drop` | `retry` | После падения |
|---|---|---|---|---|
| [`Auto`](../components/settlement/auto.md) (по умолчанию) | ничего | ничего | сообщает, что не поддерживается, и запись не возвращается | хвост обработанных записей читается заново, или сохранённые, но не обработанные записи пропускаются |
| [`Tracked`](api:ruststream_rdkafka::Commit::Tracked) | завершает смещение | завершает смещение | запись возвращается | каждая незавершённая запись читается заново |
| [`Transactional`](api:ruststream_rdkafka::Commit::Transactional) | завершает смещение, которое фиксируется с транзакцией конвейера | то же, что `ack` | задерживает окно до срока транзакции | открытое окно читается заново |

Пример - сервис выплат: выплаты терять нельзя, а просмотры страниц можно.
Третий режим описан в руководстве [Транзакции и exactly-once](exactly-once.md).

## At-least-once с `Commit::Tracked`

`pay_out` отправляет выплату через банк и отвечает подтверждением.
Если банк не ответил, выплата возвращается и обрабатывается снова.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/commits.rs#tracked"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#tracked"
```

</TabItem>
</Tabs>

При [`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) `ack` поднимает зафиксированную позицию до самой ранней доставки, которая ещё не завершена.
Подтверждения от параллельных воркеров приходят в любом порядке, а позиция никогда не обгоняет выплату, которая ещё выполняется.
[`retry`](api:ruststream::runtime::HandlerOutcome::retry) возвращает доставку подписке, и та доставляет её снова раньше всего, что выберет дальше.
До тех пор её смещение остаётся незавершённым, поэтому позиция за него не уходит.
Если процесс остановится раньше, после нового запуска Kafka вернёт ему эту выплату.

Поэтому выплата никогда не теряется, но может быть обработана дважды: до падения и после него.
Вызов банка должен принимать повторную выплату, например распознавая её по идентификатору.
`retry` без предела попыток возвращает запись бесконечно. Предел добавляет руководство [Повторы и темы dead-letter](retries.md).

## Собственная фиксация librdkafka

`count_view` считает просмотры страниц в хранилище аналитики.
Потерянный просмотр здесь не важен, поэтому подписка остаётся в режиме по умолчанию.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/commits.rs#auto"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#auto"
```

</TabItem>
</Tabs>

При [`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) librdkafka сохраняет позицию, когда отдаёт запись приложению, и фиксирует её по таймеру.
`ack` и `drop` ничего не меняют.
`retry` сообщает, что не поддерживается, и просмотр не возвращается.
Этот режим самый дешёвый. Зато при падении можно потерять обработанные записи или пропустить записи, которые были сохранены, но не обработаны.

## Пакеты

`reconcile` принимает строки выписки [пакетами](../components/subscriptions/batches.md) до 500 штук и завершает каждую строку отдельно.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/commits.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#batch"
```

</TabItem>
</Tabs>

Пакет - это одна доставка и записи, которые librdkafka уже выбрала, без дополнительного ожидания. Поэтому он часто меньше размера, заданного в точке монтирования.
Возвращённый список завершает пакет поэлементно. В ручной форме `Ok(())` подтверждает все элементы, а `Err(outcomes)` завершает каждый по-своему.
При `Commit::Tracked` элемент, который просит повтор, возвращается один, и зафиксированная позиция стоит перед ним, пока он не завершится.
Если процесс остановится раньше, подтверждённые элементы после него будут прочитаны снова: это дубликаты, а не потери.

Сколько записей librdkafka держит в очереди, определяет свойство консьюмера, которое задаёт [`KafkaTopic::config`](../components/subscriptions/passthrough.md).
Когда очередь полна, librdkafka приостанавливает выборку на `fetch.queue.backoff.ms` (по умолчанию 1000 мс), и большие записи за медленным обработчиком замедляются.
`.config("fetch.queue.backoff.ms", "10")` сокращает эту паузу.

## Сторона продюсера

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/commits.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/commits.rs#app"
```

</TabItem>
</Tabs>

At-least-once от начала до конца требует и стороны продюсера.
`enable.idempotence` делает так, что собственные повторные отправки librdkafka записывают запись один раз. Это и любое другое свойство продюсера, включая [`acks`](../components/publishing/durability.md), задаёт [`producer_config`](../components/broker/config.md).
Каждая публикация ждёт от кластера отчёта о доставке, поэтому публикация, вернувшая `Ok`, принята Kafka.

Пока локальная очередь librdkafka полна, публикация ждёт места в ней, и обработчик замедляется до скорости, которую принимает кластер.
[`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) ограничивает это ожидание и превращает его в ошибку.
При остановке брокер дописывает записи, которые ещё в пути, не дольше [`flush_timeout`](../components/broker/shutdown.md) (по умолчанию 30 секунд).
[`ClosedKafkaBroker::unflushed_records`](api:ruststream_rdkafka::ClosedKafkaBroker::unflushed_records) считает записи, которые не успели уйти.

Тесты в файле примера запускают приложение внутри процесса и заменяют банк и хранилище аналитики клиентами, которые один раз отказывают.
[Kafka внутри процесса](../components/testing/index.md) фиксирует смещения так же, как кластер, поэтому при `Commit::Tracked` выплата возвращается, а при `Commit::Auto` просмотр страницы нет.
