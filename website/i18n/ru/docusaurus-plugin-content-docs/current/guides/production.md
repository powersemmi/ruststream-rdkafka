---
title: Подключение к промышленному кластеру
sidebar_position: 10
description: "Подключение сервиса к промышленному кластеру Kafka: SASL и TLS, надёжность продюсера, тайм-ауты, настройки подписок и тест конфигурации."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Подключение к промышленному кластеру

Промышленный кластер требует [аутентификации и TLS](../components/broker/security.md), а сервис настраивает [надёжность](../components/publishing/durability.md) своего продюсера и собственные тайм-ауты.

## SASL поверх TLS

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
ruststream-rdkafka = { version = "0.7", features = ["ssl"] }
```

Адрес и учётные данные развёртывание передаёт в переменных окружения.
Любое свойство librdkafka задаёт [`KafkaBroker::config`](../components/broker/config.md), и оно применяется к консьюмерам и к продюсеру.
`security.protocol` выбирает SASL поверх TLS, а `sasl.mechanism` - механизм SASL.
Для TLS librdkafka должна быть собрана с OpenSSL: фича `ssl` подключает системный OpenSSL, а `ssl-vendored` собирает его из исходников.
SCRAM и OAUTHBEARER требуют той же фичи, потому что librdkafka строит их на OpenSSL.
SASL `PLAIN` поверх `SASL_PLAINTEXT` никакой фичи не требует.
Kerberos (`GSSAPI`) - это фича `gssapi` крейта [rust-rdkafka](https://docs.rs/rdkafka): добавьте `rdkafka` в свои зависимости с этой фичей.
Кластеру с TLS без SASL нужен `security.protocol` со значением `SSL`, а сертификаты называют свойства `ssl.*`.

## Надёжность и тайм-ауты

[`producer_config`](api:ruststream_rdkafka::KafkaBroker::producer_config) задаёт свойство только продюсеру.
`acks=all` заставляет кластер подтверждать запись, когда её получили все синхронные реплики.
`enable.idempotence` не даёт повторной отправке продюсера записать запись дважды.
Каждая публикация ждёт подтверждения от кластера, поэтому успешная публикация означает, что кластер принял запись.

У брокера есть собственные тайм-ауты:

- [`connect_timeout`](../components/broker/startup.md) ограничивает проверку доступности кластера при запуске.
- [`flush_timeout`](../components/broker/shutdown.md) ограничивает дописывание неотправленных записей при остановке.
- [`assignment_timeout`](api:ruststream_rdkafka::KafkaBroker::assignment_timeout) ограничивает, сколько запуск ждёт, пока группа назначит подписке её партиции.

## Сервис

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

Когда локальная очередь librdkafka полна, публикация ждёт в ней места.
[`queue_timeout`](api:ruststream_rdkafka::KafkaPublish::queue_timeout) в [политике публикации](../components/publishing/index.md) превращает более долгое ожидание в ошибку.

Подписка задаёт собственные свойства через [`KafkaTopic::config`](../components/subscriptions/passthrough.md), и они важнее свойств брокера.
Два свойства принадлежат [режиму фиксации](../components/settlement/index.md): `enable.auto.commit` и `enable.auto.offset.store`.
При [`Commit::Tracked`](../components/settlement/tracked.md) и `Commit::Transactional` подписка, которая задаёт любое из них, не запускается, и ошибка называет свойство и режим.

## Проверка конфигурации

Тесты запускают рабочий `service` с рабочим `broker`, которому переданы тестовые учётные данные, через `TestApp::start`.
[Kafka внутри процесса](../components/testing/index.md) проверяет конфигурацию так же, как librdkafka, но к кластеру не подключается.
Имя или значение свойства, которое librdkafka отвергает, проваливает запуск с сообщением librdkafka:

```text
Invalid value "SASL_TLS" for configuration property "security.protocol"
```

Тестам нужны те же фичи, что и сервису.
librdkafka, собранная без OpenSSL, отвергает значения `SASL_SSL` и `SSL` у `security.protocol`.
Сама аутентификация происходит только на настоящем кластере.
