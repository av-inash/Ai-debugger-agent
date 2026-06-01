export interface IKafkaErrorEvent {
    eventId: string;
    serviceName: string;
    severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
    errorDetails: {
        message: string;
        stack: string;
        context?: Record<string, any>;
    };
    timestamp: Date;
}
