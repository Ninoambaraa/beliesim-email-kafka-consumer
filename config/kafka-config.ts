import { Kafka, Producer, Consumer, EachMessagePayload } from "kafkajs";

export default class KafkaConfig {
  private kafka: Kafka;
  private consumer: Consumer;

  constructor() {
    this.kafka = new Kafka({
      clientId: "notification-email",
      brokers: ["localhost:9092"],
    });

    this.consumer = this.kafka.consumer({ groupId: "testing-group" });
  }

  async consume(topic: string, callback: (value: string) => void) {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic, fromBeginning: true });
      await this.consumer.run({
        eachMessage: async ({
          topic,
          partition,
          message,
        }: EachMessagePayload) => {
          if (message.value != null) {
            const value = message.value.toString();
            const data = { value, key: message.key?.toString() };
            callback(JSON.stringify(data));
          } else {
            console.warn("Received message with null or undefined value");
          }
        },
      });
    } catch (error) {
      throw error;
    }
  }

  async disconnect() {
    try {
      await this.consumer.disconnect();
    } catch (error) {
      console.error("Error disconnecting Kafka consumer:", error);
    }
  }
}
