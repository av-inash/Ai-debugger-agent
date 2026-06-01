import express, { Request, Response } from 'express';
import { IKafkaErrorEvent } from '@ai-debugger/shared-types';
import { Kafka, Partitioners } from 'kafkajs';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;

// 1. Kafka Setup
const kafka = new Kafka({
    clientId: 'order-service-client',
    brokers: ['localhost:9092'] // Docker wala Kafka port
});
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

// Ek dummy route jo error throw karega
app.post('/api/v1/orders', async (req: Request, res: Response) => {
    try {
        const { item, amount } = req.body;
        
        if (!amount) {
            throw new Error("Amount is required to place an order");
        }

        // Logic goes here...
        res.status(201).json({ message: "Order placed successfully" });

    } catch (error: any) {
        const errorEvent: IKafkaErrorEvent = {
            eventId: `evt_${Date.now()}`,
            serviceName: 'order-service',
            severity: 'HIGH',
            errorDetails: {
                message: error.message,
                stack: error.stack || '',
                context: { requestBody: req.body }
            },
            timestamp: new Date()
        };

        // 2. Error ko Kafka Topic par push karna
        try {
            await producer.send({
                topic: 'global-error-stream',
                messages: [
                    { value: JSON.stringify(errorEvent) }
                ],
            });
            console.log("✅ [SUCCESS] Error directly sent to Kafka Topic: global-error-stream");
        } catch (kafkaError) {
            console.error("❌ [KAFKA FAILED] Could not send message to Kafka", kafkaError);
        }

        res.status(500).json({ error: "Internal Server Error" });
    }
});

// 3. Server start karne se pehle Kafka Producer connect karna
const startServer = async () => {
    try {
        await producer.connect();
        console.log("🔗 Connected to Kafka Producer successfully");
        
        app.listen(PORT, () => {
            console.log(`📦 Order Service is running on http://localhost:${PORT}`);
        });
    } catch (error) {
        console.error("Failed to start server:", error);
    }
};

startServer();