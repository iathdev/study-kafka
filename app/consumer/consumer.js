const express = require('express');
const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

const app = express();
const kafka = new Kafka({
    clientId: 'kafka-consumer',
    brokers: ['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
});

const schemaRegistry = new SchemaRegistry({ host: 'http://schema-registry:8081' });

const consumer = kafka.consumer({ groupId: 'test-group' });

(async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const decodedMessage = await schemaRegistry.decode(message.value);

            console.log({
                value: decodedMessage.value,
                topic,
                partition,
            });
        },
    });

    console.log('Consumer is listening...');
})();

app.get('/health', (req, res) => {
    res.json({ status: 'Kafka consumer is running' });
});

app.listen(3001, () => {
    console.log('Kafka Consumer API running at http://localhost:3001');
});
