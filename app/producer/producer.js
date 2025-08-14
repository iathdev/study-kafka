const express = require('express');
const { Kafka, Partitioners } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

const app = express();
app.use(express.json());

const kafka = new Kafka({
    clientId: 'kafka-producer',
    brokers: ['kafka-1:9092', 'kafka-2:9093', 'kafka-3:9094'],
    retry: {
        maxRetryTime: 30000,
        initialRetryTime: 1000,
        retries: 10,
    },
});

const schemaRegistry = new SchemaRegistry({ host: 'http://schema-registry:8081' });

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner
});

async function connectWithRetry() {
    let retries = 10;
    while (retries > 0) {
        try {
            await producer.connect();
            console.log('🚀 Producer connected');
            return;
        } catch (err) {
            console.error('Failed to connect, retrying...', err);
            retries--;
            await new Promise(resolve => setTimeout(resolve, 5000)); // Chờ 5 giây trước khi thử lại
        }
    }
    console.error('Could not connect to Kafka after retries');
    process.exit(1);
}

(async () => {
    await connectWithRetry();
})();

app.post('/produce', async (req, res) => {
    const { message } = req.body;

    try {
        // Định nghĩa schema Avro
        const schema = {
            type: 'record',
            name: 'Message',
            namespace: 'com.example.kafka',
            fields: [{ name: 'value', type: 'string' }],
        };

        // Đăng ký schema
        const { id } = await schemaRegistry.register({
            type: 'AVRO',
            schema: JSON.stringify(schema),
            compatibility: 'BACKWARD',
        });

        // Mã hóa message
        const encodedMessage = await schemaRegistry.encode(id, { value: message });

        await producer.send({
            topic: 'test-topic',
            messages: [{ value: encodedMessage }],
        });

        console.log(`Message sent: ${message}`);

        res.json({ status: 'Message sent', message });
    } catch (err) {
        console.error('Error sending message:', err);
        res.status(500).json({ error: err.message });
    }
});

app.listen(3000, () => {
    console.log('📡 Kafka Producer API running at http://localhost:3000');
});