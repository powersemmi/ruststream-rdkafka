---
title: Тестирование сервиса на Kafka
sidebar_position: 9
description: "Тестирование сервиса на Kafka внутри процесса с TestApp: рабочее приложение, записи с ключом, ответы, задержки и тот же тест на кластере."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на Kafka

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [Kafka внутри процесса](../components/testing/index.md) или на кластере.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в тему `card-payments`, и ключ его записи - счёт для списания.
Обработчик отвечает квитанцией в тему `card-receipts`.
Платёж без ключа отбрасывается.
Платёж сверх мгновенного лимита банка откладывается: обработчик просит доставить его снова через минуту с [`retry_after`](../components/settlement/retries.md).
После трёх доставок платёж уходит в `card-payments.review`, [тему dead-letter](../components/settlement/dead-letters.md).
Приложение строит `service(broker)`, поэтому тест выбирает брокер сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-rdkafka` в `[dev-dependencies]`.

## Платёж с ключом

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`KafkaBroker`](../components/broker/index.md) к Kafka внутри процесса.
Соединение по начальному адресу не устанавливается.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<KafkaBroker>()`.
Ключ записи Kafka передаётся заголовком [`PARTITION_KEY_HEADER`](../components/publishing/keys.md), поэтому тест задаёт его через `with_headers`.
Брокер превращает этот заголовок в настоящий ключ записи Kafka, а обработчик читает его обратно через [`Ctx<Key>`](../components/context/index.md).
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик завершил доставку и что дошло до `card-receipts`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и тест на кластере ниже.

## Платёж без ключа

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#keyless"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#keyless"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Задержка без ожидания

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` не стоит ничего.
Kafka не умеет придерживать запись, поэтому по окончании задержки `retry_after` публикует копию записи в её тему.
Копия несёт число попыток в заголовке, и `published` показывает её в `card-payments` рядом с собственной записью теста.
Третья доставка - последняя, которую допускает точка монтирования, поэтому запись уходит в `card-payments.review` в том виде, в каком пришла.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует Kafka внутри процесса

Kafka внутри процесса читает ту же конфигурацию, что и librdkafka: [`config`](../components/broker/config.md) и `producer_config` брокера, параметры каждого дескриптора и значения librdkafka по умолчанию.
Свойство, которое librdkafka отвергает, так же проваливает запуск теста.
Она моделирует Kafka, а не очередь:

- Каждая группа консьюмеров читает запись один раз, и каждая группа читает свою копию.
- [Режим фиксации](../components/settlement/index.md) решает, что фиксируется: `retry` при [`Commit::Tracked`](api:ruststream_rdkafka::Commit::Tracked) возвращает запись, а при [`Commit::Auto`](api:ruststream_rdkafka::Commit::Auto) сообщает, что не поддерживается.
- Ключ записи определяет её размещение и возвращается в обработчик.
- [Перемотка](../components/positions/seek.md) переносит подписку на смещение, на любой конец журнала или на отметку времени.
- Читатель `read_committed` видит записи [транзакции](../components/transactions/index.md) после её фиксации и никогда после прерывания.

У каждой темы там одна партиция.
Для нескольких партиций, кооперативной перебалансировки, срока хранения и времени входа в настоящую группу нужен кластер.

## Тот же тест на кластере

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live_within`](../components/testing/live.md) подключает то же приложение к работающей Kafka.
Его срок - самое долгое время, которое публикация ждёт завершения реакции. Настоящему кластеру нужно больше, чем значение по умолчанию внутри процесса.
Тест читает адрес из `KAFKA_TEST_URL` через `live::url`, собственную вспомогательную функцию примера, и пропускается, если переменная не задана.
Он входит в собственную группу консьюмеров, поэтому не читает ничего, что оставил прошлый прогон.
Сначала он создаёт `card-payments`: подписка открывается на партициях, которые есть у темы, а тема, появившаяся позже, читается с того места, куда её ставит `auto.offset.reset`.
Проверки те же, что и внутри процесса, без изменений.

```bash
cargo test
KAFKA_TEST_URL=localhost:9092 cargo test -- --test-threads=1
```
