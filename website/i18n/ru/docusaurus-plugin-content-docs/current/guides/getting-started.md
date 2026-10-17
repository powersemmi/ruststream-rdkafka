---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-rdkafka, локальная Kafka, первый подписчик на ней и его тест без кластера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-rdkafka`, запускает локальную Kafka и первый сервис на ней.

## Установка

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-rdkafka = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-rdkafka` - брокер Kafka.
Его фича `testing` добавляет [Kafka внутри процесса](../components/testing/index.md), на которой работают тесты.

У крейта брокера такие фичи:

| Фича | Что она даёт |
|---|---|
| `json` *(по умолчанию)*, `msgpack`, `cbor` | одноимённый кодек |
| `schema-registry` | клиент Confluent [Schema Registry](../components/schema-registry/index.md) и его формат данных |
| `avro` | [Avro](../components/avro/index.md) поверх Schema Registry |
| `protobuf` | сообщения [Protobuf](../components/protobuf/index.md) в конверте Confluent |
| `asyncapi` | [привязки Kafka](../components/asyncapi/index.md) в генерируемом документе AsyncAPI |
| `testing` | Kafka внутри процесса, на которой работает `TestApp` |
| `ssl` | [TLS](../components/broker/security.md) через системный OpenSSL |
| `ssl-vendored` | TLS через OpenSSL, собранный из исходников |
| `zstd` | кодек сжатия `zstd` |

## Системные требования

Крейт собирает из исходников librdkafka, клиент Kafka на C.
Для сборки нужны компилятор C и `make`. Конфигурацию librdkafka выполняет собственный скрипт.
В Debian и Ubuntu оба инструмента ставит пакет `build-essential`.
Фиче `ssl` нужны ещё заголовки OpenSSL (`libssl-dev`), а `ssl-vendored` нужен `perl`, чтобы собрать OpenSSL.
Сборку через CMake и системную librdkafka даёт [rust-rdkafka](https://docs.rs/rdkafka). Добавьте `rdkafka` в свои зависимости с нужной фичей, и сборка её подхватит.

## Локальная Kafka

Для разработки достаточно одного узла Kafka в режиме KRaft:

```yaml title="compose.yaml"
services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

Запустите его и создайте тему, которую читает сервис:

```bash
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic orders
```

Брокер слушает `localhost:9092`.

## Первый сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из темы `orders`.
Заказ с суммой он подтверждает, а заказ без суммы отбрасывает.
[`KafkaBroker::new`](../components/broker/index.md) принимает начальные адреса кластера и только запоминает их.
Соединение открывается при запуске сервиса.
Kafka читает тему только в составе группы консьюмеров.
[`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group) - группа каждой подписки, которая не называет свою.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте запись с заказом из другого терминала консольным продюсером, который входит в образ:

```bash
echo '{"id":1,"amount_cents":1250}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
```

Сервис пишет в журнал `order taken` с номером заказа.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

`cargo generate` создаёт такой же сервис с роутером и ответом:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

## Первый тест

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `KafkaBroker` подключается к Kafka внутри процесса, поэтому тестам не нужен кластер:

```bash
cargo test
```

Руководство [Тестирование сервиса на Kafka](testing.md) разбирает эти тесты и то, что моделирует Kafka внутри процесса.

## Переход с rust-rdkafka

Сервис на rust-rdkafka сам пишет цикл консьюмера и продюсер.
Вот куда переходит каждая часть в RustStream:

| rust-rdkafka | RustStream |
|---|---|
| `ClientConfig::new().set("group.id", ..)` | [`default_group`](api:ruststream_rdkafka::KafkaBroker::default_group) или [`KafkaTopic::group`](../components/subscriptions/kafka-topic.md) для отдельной подписки |
| `ClientConfig::set` для любого другого свойства | [`KafkaBroker::config`](../components/broker/config.md), [`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config), [`KafkaTopic::config`](api:ruststream_rdkafka::KafkaTopic::config) |
| `StreamConsumer::subscribe` и цикл `recv` | обработчик `#[subscriber]` |
| десериализация `message.payload()` | тип параметра обработчика, который декодирует кодек |
| `store_offset` или `commit_message` после работы | [`Commit::Tracked`](../components/settlement/tracked.md) и исход обработчика ([Фиксация и гарантии доставки](commits.md)) |
| `FutureProducer::send` | ответ или параметр `Out`, отправленный через [политику публикации](../components/publishing/index.md) |
| `seek` и `assign` | [`start_at`](../components/positions/start-at.md) в точке монтирования или [ручка перемотки](../components/positions/seek.md) в обработчике ([Повторное чтение и перемотка](replay.md)) |
| `send_offsets_to_transaction` | [`EosPublish`](../components/transactions/exactly-once/index.md) ([Транзакции и exactly-once](exactly-once.md)) |
