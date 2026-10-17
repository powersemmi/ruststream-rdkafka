---
title: Повторы и темы dead-letter
sidebar_position: 4
description: "Повтор записи Kafka сразу или позже, предел попыток и тема dead-letter для того, что не обрабатывается, на одной теме или на нескольких."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Повторы и темы dead-letter

Это руководство повторяет запись Kafka сразу или позже, ограничивает число попыток и переносит то, что раз за разом не обрабатывается, в [тему dead-letter](../components/settlement/dead-letters.md), на одной теме или на нескольких.

Kafka не придерживает записи и не считает доставки.
И то и другое делает фреймворк: точка монтирования объявляет предел попыток и тему dead-letter, а повтор уходит копией записи с числом попыток в заголовке.

Пример - сервис возвратов, который проводит возвраты через процессинг карт.

## Подписчик

Процессинг отказывает в возврате тремя способами, и каждому нужен свой исход.
`Busy` значит, что его лимит запросов исчерпан. Повтор прямо сейчас снова получит отказ, поэтому [`retry_after`](../components/settlement/retries.md) ждёт 30 секунд.
`Unavailable` значит, что процессинг не ответил: `retry` пробует снова сразу.
`Declined` значит, что карточный счёт закрыт. Новая попытка получит тот же ответ, поэтому возврат отбрасывается.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

[`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) завершает запись и по истечении задержки публикует её копию обратно в `refunds`.
Копия несёт заголовок с числом попыток ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), увеличенным на единицу.
Отложенная копия доставляется at-most-once: процесс, который остановился до конца задержки, её теряет.
Копия - новая запись в конце темы, поэтому записи после возврата её не ждут.

[`retry`](api:ruststream::runtime::HandlerOutcome::retry) при объявленном пределе тоже становится копией, которая публикуется сразу.
Без предела он возвращает запись подписке при [`Commit::Tracked`](../components/settlement/tracked.md), как показывает руководство [Фиксация и гарантии доставки](commits.md).

## Предел попыток и тема dead-letter

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) считает все доставки одного возврата, включая первую.
Пятая доставка не повторяется: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) публикует её в `refunds.dlq` с полезной нагрузкой и заголовками в том виде, в каком они пришли.
Отброшенный возврат завершается и никуда не уходит. В теме dead-letter лежат только возвраты, у которых кончились попытки.
Темы для повторов и для dead-letter - инфраструктура сервиса. Создайте их, как любую другую тему, а фреймворк только публикует в них.

## Чтение темы dead-letter

Тема dead-letter - обычная тема, и `review` читает её, как любую другую.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

Заголовок с числом попыток у записи из темы dead-letter хранит число доставок, через которые она прошла, здесь 5.
`review` читает его через `ctx.headers()` и пишет возврат в журнал, чтобы его посмотрел человек.

## Повторы на нескольких темах

Копия записи публикуется в тему, которую читает подписка.
[`KafkaTopic`](../components/subscriptions/kafka-topic.md) читает одну тему, поэтому ей больше ничего не нужно.
[`KafkaTopics`](../components/subscriptions/kafka-topics.md) и [`KafkaPartitions`](../components/subscriptions/kafka-partitions.md) читают набор, до которого не дотянется ни одна публикация. Поэтому их регистрация называет, куда уходит копия, а без этого сервис не запускается.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#regional"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#regional"
```

</TabItem>
</Tabs>

Приложение монтирует `process_regional_refund` с `.out_retry(Publish::default()).transform(ToSourceTopic)`.
[`ToSourceTopic`](../components/settlement/to-source-topic.md) отправляет каждую копию обратно в тему, из которой пришла её доставка, поэтому повторённый возврат из `refunds.us` возвращается в `refunds.us`.
Тему он берёт из [контекста Kafka](../components/context/index.md), поэтому обработчик принимает ключ контекста, например `Ctx<Topic>`. В ручной форме тип его контекста - `KafkaContext`.
`.out_retry(Publish::default()).to("refunds.retry")` вместо этого отправляет все копии в одну вашу тему. Добавьте эту тему в подписку, чтобы копии возвращались.
То же правило действует для подписки, которая никогда не повторяет: каждая регистрация на набор тем называет адрес для повторов.

## Проверка задержки

Тесты в файле примера запускают приложение внутри процесса с остановленными часами, поэтому `tb.advance(BACKOFF)` проматывает 30 секунд мгновенно.
Работу с `TestApp` описывает руководство [Тестирование сервиса на Kafka](testing.md).
