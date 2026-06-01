import express, { Request, Response } from 'express';
import { Kafka, Partitioners } from 'kafkajs';
import { IKafkaErrorEvent } from '@ai-debugger/shared-types';

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3002;

// 1. Kafka Setup
const kafka = new Kafka({
    clientId: 'payment-service-client',
    brokers: ['localhost:9092'] // Docker Kafka port
});
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

app.post('/api/v1/payments/process', async (req: Request, res: Response) => {
    try {
        const { orderId, paymentMethod } = req.body;

        // ✅ FIX: Removed the hardcoded Chaos Test error.
        // In a real production environment, you would integrate with the Stripe SDK here.
        // Example: const charge = await stripe.charges.create({...});
        
        console.log(`Processing payment for Order: ${orderId} using ${paymentMethod}`);

        // Simulate successful processing logic
        res.status(200).json({ 
            message: "Payment processed successfully",
            transactionId: `txn_${Date.now()}`,
            orderId 
        });
    } catch (error: any) {
        const errorEvent: IKafkaErrorEvent = {
            eventId: `evt_${Date.now()}`,
            serviceName: 'payment-service',
            severity: 'HIGH',
            errorDetails: {
                message: error.message,
                stack: error.stack || '',
                context: { requestBody: req.body }
            },
            timestamp: new Date()
        };

        try {
            await producer.send({
                topic: 'global-error-stream',
                messages: [{ value: JSON.stringify(errorEvent) }],
            });
            console.log("✅ [SUCCESS] Payment system error sent to Kafka Topic: global-error-stream");
        } catch (kafkaError) {
            console.error("❌ [KAFKA FAILED] Could not send message to Kafka", kafkaError);
        }

        res.status(500).json({ error: "Internal Payment Server Error" });
    }
});

const startServer = async () => {
    try {
        await producer.connect();
        console.log("🔗 Connected to Kafka Producer successfully from Payment Service");
        
        app.listen(PORT, () => {
            console.log(`💳 Payment Service is running on http://localhost:${PORT}`);
        });
    } catch (error) {
        console.error("Failed to start payment server:", error);
    }
};

startServer();
