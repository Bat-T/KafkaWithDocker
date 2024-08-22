using KafkaWithDocker.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace KafkaWithDocker.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaProducerApiController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaProducerService;

        public KafkaProducerApiController(KafkaProducerService kafkaProducerService)
        {
            _kafkaProducerService = kafkaProducerService;
        }

        [HttpPost]
        [Route("publish")]
        public async Task<IActionResult> Publish([FromBody] string message)
        {
            await _kafkaProducerService.ProduceAsync(message);
            return Ok("Message published");
        }
    }
}
