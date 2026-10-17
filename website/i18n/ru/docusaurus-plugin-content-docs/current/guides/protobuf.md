---
title: Protobuf в конверте Confluent
sidebar_position: 8
description: "Сообщения Protobuf, сгенерированные prost, в конверте Confluent: чтение оформленных записей без реестра и публикация оформленных ответов."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Protobuf в конверте Confluent

Это руководство обрабатывает сообщения Protobuf, сгенерированные prost, в [конверте Confluent](../components/schema-registry/envelope.md): читает оформленные записи без реестра и публикует оформленные ответы.

Запись Protobuf, которая опирается на реестр, начинается с конверта Confluent: нулевого байта, четырёхбайтового идентификатора схемы и пути индексов, который называет, какое сообщение схемы идёт дальше.
Сгенерированное сообщение не тип serde, поэтому через кодек оно не проходит.
Оно само себя кодирует и декодирует, а брокер надевает на него конверт и снимает его.

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["protobuf"] }
prost = "0.14"
```

Берите ту версию `prost`, от которой зависит крейт.

## Сгенерированные типы в обработчике

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#messages"
```

</TabItem>
</Tabs>

`Parcel` и `Label` - то, что `prost-build` генерирует из этой схемы, плюс derive RustStream:

```protobuf title="shipping.proto"
syntax = "proto3";
package shipping;

message Parcel {
  int64 id = 1;
  string address = 2;
}

message Label {
  int64 parcel = 1;
  string address = 2;
}
```

В скрипте сборки `message_attribute` из prost-build добавляет derive к сгенерированным структурам.
`#[derive(Deserialized)]` позволяет обработчику принимать сообщение, а `#[derive(Serialized)]` - возвращать его.
`#[wire(..)]` называет функцию, которая читает или пишет байты.
В ручной форме вместо derive стоят реализации трейтов.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#handler"
```

</TabItem>
</Tabs>

`print_label` принимает сгенерированный `Parcel` и возвращает сгенерированный `Label`.
Посылка без адреса отбрасывается, и для неё ничего не публикуется.

## Чтение оформленных записей

[`decode_confluent`](../components/protobuf/index.md) - функция декодирования `Parcel`.
Она перешагивает через конверт и путь индексов, а затем сообщение читает `prost`.
Реестр ей не нужен: сгенерированный тип и так знает свои поля.
Запись без конверта читается как голое сообщение, поэтому тема, в которую одни продюсеры пишут с конвертом, а другие без, всё равно декодируется.

## Публикация оформленных записей

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/protobuf.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/protobuf.rs#service"
```

</TabItem>
</Tabs>

`Label` пишет голый Protobuf, а политика публикации ставит перед ним конверт.
Эта политика - [`KafkaPublish::framed`](../components/protobuf/framed.md), и для ответа её называет `out_reply`.
При первой публикации в тему она находит субъект темы (`labels-value`) и кеширует идентификатор его схемы.
Путь индексов по умолчанию указывает на первое сообщение схемы.
[`message`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::message) называет для темы другое сообщение, а [`subject`](api:ruststream_rdkafka::protobuf::KafkaFramedPublish::subject) - другой субъект.

Субъект разрешается при первой публикации, а не при запуске, потому что только тогда известна тема.
Если реестр не хранит субъект или хранит под ним другой формат, публикация завершается ошибкой, и доставка завершается повтором.
В [режиме фиксации по умолчанию](../components/settlement/auto.md) повтор не доставляется снова. Режимы описаны в руководстве [Фиксация и гарантии доставки](commits.md).

[`ProtobufFrame`](../components/protobuf/protobuf-frame.md) оформляет записи для всего приложения и добавляется через `RustStream::publish_layer`.
Он оформляет то, что уходит через слоты `Out` и через издателей, которых не называют точки монтирования, если субъект адресата хранит схему Protobuf. Остальное он пропускает как есть.
Ответ идёт прямо в собственного издателя и через слой не проходит, поэтому обработчик с ответом называет `KafkaPublish::framed`.
Полезную нагрузку, у которой конверт уже есть, слой не трогает, поэтому слой и политика сочетаются.

## Тестирование внутри процесса

Чтению реестр не нужен, поэтому `print_label` читает оформленные записи под [`TestApp::start`](../components/testing/index.md) без реестра и без кластера.
Первый тест примера публикует оформленную посылку без адреса и проверяет, что она отброшена и что ни одна этикетка не опубликована.
Оформленной публикации нужен идентификатор схемы субъекта, поэтому тесты этикетки работают с реестром.
Работу с `TestApp` описывает руководство [Тестирование сервиса на Kafka](testing.md).

## Ограничения

Схему Protobuf нельзя вывести из сгенерированного типа.
Поэтому исходник `.proto` регистрируют отдельно: через [`SchemaRegistry::register`](../components/schema-registry/registration.md) с `SchemaType::Protobuf` или инструментами самого реестра.
Схема, зарегистрированная для темы с конвертом, может импортировать только стандартные типы (`google/protobuf/*.proto`). Другие ссылки на схемы не разрешаются.
