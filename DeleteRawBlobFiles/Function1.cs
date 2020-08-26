using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Extensions.Configuration;

namespace DeleteRawBlobFiles
{
    public static class Function1
    {
        private static Settings settings;
        [FunctionName("DeleteRawBlobFiles")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {
            string League = req.Query["League"];
            string Season = req.Query["Season"];
            string SeasonType = req.Query["SeasonType"];
            string Week = req.Query["Week"];
            string GameKey = req.Query["GameKey"];

            GetSettings(context, log);
            await DeleteRawBlobFiles(League.ToLower(), Season, SeasonType, Week, GameKey);

            return new OkObjectResult("Files Deleted");
        }
        private static async Task DeleteRawBlobFiles(string League, string Season, string SeasonType, string Week, string GameKey)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(settings.outputStorageAccountConnStr);
            // Connect to the blob storage
            var serviceClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer inputContainer = serviceClient.GetContainerReference($"{League}");
            CloudBlobDirectory inputDirectory = inputContainer.GetDirectoryReference($"{Season}/{SeasonType}/{Week}/{GameKey}/");
            var blobItem = await inputDirectory.ListBlobsSegmentedAsync(null);

            try
            {
                // Delete the specified container and handle the exception.
                foreach (var item in blobItem.Results)
                {
                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        var blob = (CloudBlockBlob)item;
                        await blob.DeleteAsync();
                    }
                }
            }
            catch (StorageException e)
            {
                Console.WriteLine("HTTP error code {0}: {1}",
                                    e.RequestInformation.HttpStatusCode,
                                    e.RequestInformation.ErrorCode);
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
        }
        static void GetSettings(ExecutionContext context, ILogger log)
        {
            try
            {
                var config = new ConfigurationBuilder()
                    .SetBasePath(context.FunctionAppDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                settings = new Settings();

                settings.outputStorageAccountConnStr = config["VikingsStorageAccount"];
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.Message);
            }
        }
    }
    class Settings
    {
        public string outputStorageAccountConnStr { get; set; }
    }
}
