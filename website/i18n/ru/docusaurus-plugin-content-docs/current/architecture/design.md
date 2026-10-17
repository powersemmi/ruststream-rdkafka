---
title: Устройство
sidebar_position: 2
description: "Где стоит ruststream-rdkafka: между рантаймом RustStream и клиентом rust-rdkafka, один консьюмер на подписку, один общий продюсер."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Сверху вниз: сервис, рантайм RustStream, этот крейт, клиент rust-rdkafka и кластер Kafka",
  "text": "Схема текстом",
  "service": "Ваш сервис",
  "handlers": ["#[subscriber]", "обработчики-подписчики"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "Ядро RustStream",
  "runtime": "Рантайм: диспетчер, партиции, завершение",
  "crate": "ruststream-rdkafka",
  "broker": ["KafkaBroker", "конфигурация"],
  "descriptors": ["KafkaTopic,", "KafkaTopics,", "KafkaPartitions"],
  "subscriber": ["KafkaSubscriber", "выдаёт KafkaMessage"],
  "publishers": ["KafkaPublisher", "и транзакционные", "издатели"],
  "tracker": "Учёт фиксаций",
  "eos": "EosPipeline",
  "client": "rust-rdkafka и librdkafka",
  "consumer": ["StreamConsumer", "по одному на подписку"],
  "producer": ["FutureProducer", "один на соединение"],
  "cluster": "Кластер Kafka",
  "mounted": "монтируются",
  "polls": "опрашивает поток"
};

# Устройство

Где стоит `ruststream-rdkafka`: между рантаймом RustStream и клиентом rust-rdkafka, один
консьюмер на подписку, один общий продюсер.

<Diagram name="design" words={words} />

Крейт реализует трейты брокера из ядра поверх
[rust-rdkafka](https://docs.rs/rdkafka/0.39.0/rdkafka/), обёртки над librdkafka. Рантайм над ним
один для всех брокеров. Всё, что относится к Kafka, лежит в этом крейте: дескрипторы, смысл ack,
адрес копии для повтора и то, как фиксируется транзакция.

Каждая подписка открывает собственный консьюмер librdkafka, поэтому подписка - один участник своей
группы консьюмеров. Издатели одного соединения делят один продюсер, и открывает его первая
публикация. Транзакционный издатель открывает собственный продюсер под своим `transactional.id`.

Настройка, для которой в крейте нет типа, - это свойство librdkafka. Через
[`config`](api:ruststream_rdkafka::KafkaBroker::config) можно задать любое из них, а незаданное
свойство сохраняет значение librdkafka по умолчанию.

[Учёт фиксаций](settlement.md) превращает подтверждения в сохранённые смещения.
[`EosPipeline`](api:ruststream_rdkafka::EosPipeline) фиксирует прочитанные смещения внутри
транзакции продюсера. Как части запускаются и останавливаются, показывает страница
[Жизненный цикл](lifecycle.md), а путь одной записи - страница [Путь](path.md).
