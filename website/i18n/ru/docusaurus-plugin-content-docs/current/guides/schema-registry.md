---
title: "Schema Registry: Avro и JSON Schema"
sidebar_position: 7
description: "Чтение и публикация записей Avro и JSON Schema в конверте Confluent, со сверкой каждой схемы со Schema Registry при запуске."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schema Registry: Avro и JSON Schema

Это руководство читает и публикует записи Avro и JSON Schema в [конверте Confluent](../components/schema-registry/envelope.md) и при запуске сверяет каждую схему со [Schema Registry](../components/schema-registry/index.md).

Запись, которая опирается на реестр, начинается с конверта Confluent: нулевого байта и четырёхбайтового идентификатора схемы, которой она записана.
Сами схемы хранятся в реестре.
В RustStream схема принадлежит кодеку, поэтому обработчики принимают и возвращают обычные структуры serde.

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["avro"] }
apache-avro = { version = "0.22", features = ["derive"] }
```

Фича `avro` добавляет кодек Avro поверх `schema-registry`, а та приносит клиент реестра и конверт.
`apache-avro` даёт derive `AvroSchema`. Берите ту версию, от которой зависит крейт.

## Модели

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#models"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#models"
```

</TabItem>
</Tabs>

`AvroSchema` выводит схему Avro каждой структуры.
Поле `currency` появилось во второй версии схемы счёта.
`#[avro(default)]` - значение, которое получает запись, записанная без этого поля.

## Обработчик

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#handler"
```

</TabItem>
</Tabs>

`issue_receipt` читает счёт и отвечает квитанцией в тему `receipts`.
Ничего в нём не знает ни об Avro, ни о конверте.

## Реестр, предвыборка и кодек

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#service"
```

</TabItem>
</Tabs>

[`SchemaRegistry::new`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::new) запоминает URL реестра и пока ничего не отправляет.
Все его клоны делят один [кеш](../components/schema-registry/cache.md), поэтому схема загружается один раз на процесс.
Сетевую работу выполняет [`SchemaPrefetch`](../components/schema-registry/schema-prefetch.md), а [`schema_prefetch`](api:ruststream_rdkafka::KafkaBroker::schema_prefetch) подключает его к брокеру.
Когда брокер подключается, предвыборка разрешает каждый субъект, под которым публикуют кодеки.
Когда приходит запись, предвыборка ещё до работы кодека загружает схему, которую называет конверт: ту, по которой запись записана.
Поэтому сам кодек никогда не ждёт сети.

[`AvroCodec::registry`](../components/avro/index.md) строит кодек Avro из предвыборки.
[`register`](api:ruststream_rdkafka::avro::AvroCodec::register) называет субъект, под которым оформляются значения типа: квитанции уходят под `receipts-value`.
Для декодирования регистрация не нужна, потому что каждая запись сама называет схему, по которой её записали.

`with_broker_codec` декодирует все обработчики области кодеком Avro.
Ответ кодируется кодеком, названным в его точке монтирования, а если кодек не назван, то JSON-кодеком по умолчанию.
Поэтому `out_reply(KafkaPublish::default()).codec(avro)` называет для квитанций кодек Avro.

## Чтение записей по старой схеме

[`resolve_onto`](../components/avro/resolve-onto.md) велит Avro приводить каждую запись к схеме чтения, здесь `Invoice::get_schema()`.
У записи, записанной по первой версии схемы, нет `currency`.
Приведение заполняет поле значением по умолчанию, и обработчик читает `"EUR"`.
Схема чтения действует на каждую запись, которую декодирует её кодек, поэтому один такой кодек обслуживает один тип.
Второй тип со своей схемой чтения получает собственный кодек в собственном роутере.

## Регистрация субъектов и отсутствующий субъект

Сервис не регистрирует схем: [субъект регистрирует](../components/schema-registry/registration.md) тот, кому принадлежит тема, например на шаге развёртывания.
[`register_avro`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_avro) регистрирует схему Avro типа под субъектом, [`register_json`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register_json) - его JSON Schema, а [`register`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::register) - схему, заданную текстом.

Если субъекта, под которым публикует кодек, в реестре нет, брокер не подключается, и ошибка называет субъект.
[`MissingSubject`](../components/schema-registry/missing-subject.md) выбирает, что происходит вместо этого:

| Политика | Что она делает |
|---|---|
| `Refuse` *(по умолчанию)* | сервис не запускается |
| `AutoRegister` | регистрирует под субъектом собственную схему типа и пишет предупреждение в журнал |
| `PublishUnframed` | публикует голые данные без конверта и пишет предупреждение в журнал |

Задайте её через [`on_missing_subject`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::on_missing_subject) до того, как из предвыборки будут построены кодеки.

## Совместимость при запуске

При подключении схема каждого зарегистрированного типа проверяется так, как проверялась бы при регистрации.
Уровень совместимости субъекта решает, с какими версиями она сверяется: с последней или, при уровне `_TRANSITIVE`, со всеми.
Модель, которая разошлась со своим субъектом, останавливает сервис, а описание расхождения от реестра попадает в ошибку.
[`check_compatibility(false)`](api:ruststream_rdkafka::schema_registry::SchemaPrefetch::check_compatibility) выключает проверку для субъекта с уровнем `NONE`.

## JSON Schema в конверте

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#dispute"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#dispute"
```

</TabItem>
</Tabs>

[`SchemaFramed`](../components/schema-registry/schema-framed.md) оборачивает JSON-кодек ядра в конверт.
Декодирование перешагивает через конверт и читает документ JSON внутри него.
Кодирование оформляет значение идентификатором субъекта, который [`register`](api:ruststream_rdkafka::schema_registry::SchemaFramed::register) называет для его типа, так же как с Avro.
В сервисе выше роутер даёт подписке `disputes` этот кодек, потому что побеждает самый точный кодек.
`JsonSchema` выводит JSON Schema типа, которую регистрирует `register_json`.

## Закреплённая локальная схема

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/schema_registry.rs#archive"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/schema_registry.rs#archive"
```

</TabItem>
</Tabs>

[`AvroCodec::local`](../components/avro/local.md) держит одну схему и применяет её к каждой записи.
Тема несёт голые данные Avro: на этом пути нет ни конверта, ни реестра, ни сети.
Поэтому сервис архива работает и тестируется без реестра.

## Путь с перекодированием

Сервис может и оставить обычные модели serde на JSON-кодеке по умолчанию, а преобразовывать данные на границах брокера.
[`KafkaBroker::schema_registry`](../components/schema-registry/json-transcode.md) переводит каждую оформленную доставку в JSON до работы кодека.
[`SchemaFrame`](api:ruststream_rdkafka::schema_registry::SchemaFrame), добавленный через `RustStream::publish_layer`, оформляет каждую исходящую полезную нагрузку JSON в формате, который хранит субъект её адресата: Avro, JSON Schema или Protobuf.
Тема без субъекта в реестре публикуется как есть.
Этот путь стоит одного преобразования JSON на сообщение и не приводит записи к схеме чтения.
Брокер работает по одному из двух путей: при подключённом `schema_registry` кодек получал бы JSON.

## Другой крейт реестра

[`SchemaRegistry::with_client`](api:ruststream_rdkafka::schema_registry::SchemaRegistry::with_client) принимает собственный [`RegistryClient`](../components/schema-registry/registry-client.md), например поверх другого HTTP-стека или другого клиентского крейта.
Тогда кодеки, предвыборка и путь с перекодированием обращаются к реестру через него.

Крейт, который сам делает [всю работу со схемами](../components/schema-registry/third-party.md), например `schema_registry_converter`, работает с байтами.
Обработчик принимает байты записи через тип с `#[derive(Deserialized)]` и возвращает закодированный ответ через тип с `#[derive(Serialized)]`.
Между таким крейтом и RustStream передаются только байты, поэтому его версия `apache-avro` не обязана совпадать с версией этого крейта.
