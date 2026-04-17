using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Xelerate;

public static class KafkaExtension
{
    public static async Task EnsureTopicsExistAsync(string kafkaBootstrapServers)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafkaBootstrapServers }).Build();
    
        try
        {
            // Khai báo 2 topic cần thiết
            var topics = new List<TopicSpecification>
            {
                new() { Name = "XelerateServerTopic", NumPartitions = 1, ReplicationFactor = 1 },
                new() { Name = "XelerateClientTopic", NumPartitions = 1, ReplicationFactor = 1 }
            };

            await adminClient.CreateTopicsAsync(topics);
        }
        catch (CreateTopicsException e)
        {
            // Bỏ qua lỗi nếu Topic đã tồn tại từ lần test trước
            if (e.Results[0].Error.Code != ErrorCode.TopicAlreadyExists)
            {
                Console.WriteLine($"Lỗi khi tạo topic: {e.Results[0].Error.Reason}");
            }
        }
    }
}