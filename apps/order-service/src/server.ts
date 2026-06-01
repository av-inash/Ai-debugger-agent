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

// Route to place an order with proper validation
app.post('/api/v1/orders', async (req: Request, res: Response) => {
    try {
        const { item, amount } = req.body;
        
        // Validation: Return 400 Bad Request for missing fields instead of throwing a 500 error
        if (!amount) {
            return res.status(400).json({ 
                error: "Bad Request", 
                message: "Amount is required to place an order" 
            });
        }

        if (!item) {
            return res.status(400).json({ 
                error: "Bad Request", 
                message: "Item name is required to place an order" 
            });
        }

        // Business Logic goes here...
        console.log(`Processing order for ${item} with amount ${amount}`);
        
        res.status(201).json({ message: "Order placed successfully" });

    } catch (error: any) {
        // This block now only handles genuine runtime/system failures
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
            console.log("✅ [SUCCESS] System error sent to Kafka Topic: global-error-stream");
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
