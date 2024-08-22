using Confluent.Kafka;
using System.Net;

namespace KafkaWithDocker.Services
{
    public class KafkaProducerService
    {
        private readonly IProducer<Null, string> _producer;
        private readonly string _topic;
        private readonly ProducerConfig config;

        public KafkaProducerService(string bootstrapServers, string topic)
        {
            config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = Dns.GetHostName(),
                Acks = Acks.All,
                MessageSendMaxRetries = 2, // Retry up to 5 times
                RetryBackoffMs = 200, // Wait 200ms between retries
                MessageTimeoutMs = 60000 // Set message timeout to 60 seconds
            };

            _producer = new ProducerBuilder<Null, string>(config).Build();
            _topic = topic;
        }

        public async Task ProduceAsync(string message)
        {

            try
            {
                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    try
                    {
                        var deliveryResult =await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
                        Console.WriteLine($"Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Delivery failed: {e.Message}");
            }
        }
    }

}
