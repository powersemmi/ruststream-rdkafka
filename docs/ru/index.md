# Брокер Kafka

`ruststream-rdkafka` запускает сервис [RustStream](https://github.com/powersemmi/ruststream) на
Apache Kafka через [rdkafka](https://docs.rs/rdkafka) / librdkafka.

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-rdkafka = "0.7"
serde = { version = "1", features = ["derive"] }
```

Минимальный сервис - это один обработчик и одна функция приложения:

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-rdkafka/examples/kafka_quickstart.rs:app"
```

## Модель транспорта {#the-transport-model}

- Подписка - это один консьюмер, который читает одну тему. Её описывает
  [`KafkaTopic`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing), вместе с группой консьюмеров; форма с голой строкой,
  `#[subscriber("orders")]`, берёт группу из `default_group` брокера.
- Имя, объявленное исходящим сообщением, - это тема-адресат. Тип ответа с
  `#[outgoing(name = "confirmations")]` публикуется в `confirmations`, а подписчик пишет `publish`
  без имени. Тип ответа, который имени не объявляет, публикуется в тему, названную точкой
  монтирования: `publish("enriched-orders")`. Заголовок с ключом партиционирования становится
  родным ключом записи, поэтому порядок для одного ключа держит сам Kafka (см.
  [Публикацию](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing)).
- Завершение идёт по зафиксированной позиции Kafka, а не по кадру отдельного сообщения. Режим
  по умолчанию `Commit::Auto` оставляет эту позицию автоматической фиксации librdkafka;
  `Commit::Tracked` делает каждый `ack` точным подтверждением одного сообщения поверх непрерывной
  отметки. См. [Темы и группы](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing).
- Настройка передоверена librdkafka. Незаданная опция сохраняет умолчание librdkafka, а
  `config(key, value)` на брокере и на дескрипторе и `producer_config(key, value)` для продюсера
  дают доступ к любому свойству, которое крейт не вынес в типизированную опцию.

## Жизненный цикл брокера {#the-lifecycle-ladder}

Каждое состояние соединения - отдельный тип, поэтому вызов не в том порядке не компилируется:

```text
KafkaBroker::new(servers)          только настройка, синхронно, без ввода-вывода
  |
  | .connect().await?              опрашивает кластер
  v
ConnectedKafkaBroker               на нём держатся подписки и живые издатели
  |
  | .shutdown().await?             досылает незавершённые публикации
  v
ClosedKafkaBroker                  терминальный свидетель: unflushed_records()
```

`KafkaBroker::new` только запоминает настройку, поэтому сервис собирается синхронным билдером
`#[ruststream::app]`. Рантайм один раз вызывает `connect` на старте, открывает на подключённом
брокере все подписки, а в конце закрывает его.

Гарантия времени компиляции принадлежит владельцу дескриптора. Дескрипторы, которые делят с ним
соединение (издатели, инстанцированные раньше, ещё открытые подписчики), после остановки
возвращают `KafkaError::Closed`, а не выполняются успешно поверх мёртвого соединения.

Издатели устроены так же. `KafkaPublish` - это **политика**, которая конструирует **живой**
`KafkaPublisher`; `transactional_id` превращает её в транзакционную политику, `per_partition`
превращает ту в политику на партицию, а `KafkaEosPublish` - политика конвейера exactly-once.

Политику вы указываете при регистрации обработчика (`b.include(handler).out_reply(policy)` для
ответа, `.out(marker, policy)` для слота `Out<..>`), и на старте политика инстанцирует издателя на
подключённом брокере. Обработчик, который только отвечает, не называет ничего: издателя
конструирует политика брокера по умолчанию.

## Совместимости {#capabilities}

Необязательные трейты-совместимости фреймворка и то, какие из них этот брокер реализует нативно:

| Совместимость | Нативно | Подробности |
|---|---|---|
| `Subscribe` | да | `#[subscriber("orders")]` подписывается по одному имени темы, в [группе консьюмеров брокера по умолчанию](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing). |
| `BatchSubscriber` | да | Читайте целыми пакетами: одна доставка плюс всё, что librdkafka уже вычитал, без дополнительного ожидания и не больше размера, названного точкой монтирования, - [Пакеты](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#batches). |
| `TransactionalPublisher` | да | Публикуйте внутри транзакций Kafka, по одной открытой транзакции на дескриптор: [Транзакции](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#transactions). |
| `OwnedTransactions` | нет | Продюсер Kafka держит одновременно одну транзакцию на стороне брокера, поэтому транзакция не может быть отдельным значением во владении; параллельные потоки берут [издателей на партицию](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#transactions) или [конвейер exactly-once](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#exactly-once-pipelines). |
| `RequestReply` | нет | В Kafka нет сопоставления ответа с запросом; запрос-ответ строится на собственной теме ответов и заголовке корреляции. |
| `Partitioned` | да | Упорядоченные партиции воркеров с ключом по исходной партиции доставки или по ключу записи при `LaneKey::RecordKey`: [Партиции воркеров по ключу](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing). |
| `Seekable` + `Positioned` | да | Перемещайте партиции, которыми владеет этот консьюмер, прямо из обработчика - через ключ контекста `SeekHandle` рядом с `Position` самой доставки: [Перемотка подписки](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#positions-and-seeking). |
| `DescribeServer` | да | Сгенерированный документ AsyncAPI перечисляет bootstrap-серверы под протоколом `kafka`, а рядом с ними - реестр схем: [Документ AsyncAPI](#the-asyncapi-document). |

## Документ AsyncAPI {#the-asyncapi-document}

`asyncapi gen` печатает документ, которым сервис описывает себя, а этот крейт заполняет в нём
собственный словарь Kafka - спецификация называет его привязкой `kafka`. Включается он одной
возможностью:

```toml
ruststream-rdkafka = { version = "0.7", features = ["asyncapi"] }
```

После этого сервер сообщает реестр схем, с которым он настроен, канал - тему, которая за ним
стоит, а операция `receive` - группу консьюмеров, которая её читает:

```json
--8<-- "docs/snippets/asyncapi-bindings.json"
```

Идентификатор клиента появляется, когда сырой проброс дескриптора задаёт `client.id`. Группа
появляется, только если её назвал сам дескриптор: документ строится до подключения, из одного
дескриптора, который никогда не видит брокера и его `default_group`. Поля `protocolVersion` в
документе нет: версию протокола Kafka клиент и кластер согласуют отдельно по каждому ключу API,
поэтому одного числа, описывающего разговор, не существует.

Подписка `KafkaTopics` сообщает свою группу и не сообщает темы: поле `topic` в привязке называет
одну, а такая подписка читает множество. Издатель, работающий через реестр, добавляет привязку
сообщения: идентификатор схемы едет в теле в кодировке Confluent, под субъектом, который нашла его
стратегия именования.

Канал, в который сервис публикует, тоже сообщает свою тему. Эта тема - назначение, которое
разрешила точка монтирования: оговорка `publish("dest")` регистрации, собственное имя типа ответа
из `#[outgoing(name)]`, имя записи слота, объявленная тема dead-letter. Политика публикации несёт
настройки продюсера и не несёт назначения, поэтому своей темы у неё нет.

Адрес реестра попадает в документ без пользовательской части - по той же причине, что и
bootstrap-адреса: документ публикуют и передают дальше, а пароль, попавший в него, уже покинул
сервис.

## Каркас сервиса {#scaffold-a-service}

```text
cargo generate --git https://github.com/powersemmi/ruststream-rdkafka templates/kafka-topic --name my-service
```

Заготовка связывает один брокер Kafka с группой консьюмеров по умолчанию, подписчика с точной
фиксацией смещений под объявленным пределом попыток и темой dead-letter, и опубликованный ответ. Её точка входа
`#[ruststream::app]` даёт бинарнику команды `run` и `asyncapi gen`.

## Где искать документацию {#where-the-documentation-is}

Справочник лежит на docs.rs, по разделу на задачу.
[Подписка](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#subscribing) - дескрипторы, группы консьюмеров, стартовые смещения, режимы
фиксации, партиции воркеров по ключу, пакеты, повторы и перемотка.
[Публикация](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#publishing) - политики, ключи записей и явные партиции, гарантии доставки,
транзакции и конвейеры exactly-once.
[`schema_registry`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/schema_registry/index.html) - конверт Confluent, рядом с ним
[`avro`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/avro/index.html) и [`protobuf`](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/protobuf/index.html), а
[Тестирование](https://docs.rs/ruststream-rdkafka/latest/ruststream_rdkafka/index.html#testing) - само приложение сервиса под `TestApp`, внутри процесса и на
работающей Kafka.

Установка, учебник и список брокеров - на собственном сайте фреймворка:
<https://powersemmi.github.io/ruststream/>.
