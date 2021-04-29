using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using KubeMQ.SDK.csharp.QueueStream;

namespace QueusStream
{
   class Program
    {
        static int tasks = 20;
        static int send = 10000;
        static string address = "localhost:50000";
        static int rounds = 100;
        static string queue = "f";
        static KubeMQ.SDK.csharp.QueueStream.QueueStream queuesClient = new QueueStream(address, "queue-receiver", null);

    static private async Task<long> GetPollMessages(int id)
    {
        long time = 0;
            await Task.Run(async () =>
            {
                Stopwatch watch = new Stopwatch();
                try
                {
                    PollRequest pollRequest = new PollRequest()
                    {
                        Queue = $"{queue}.{id + 1}",
                        WaitTimeout = 1000,
                        MaxItems = 5000,
                    };
                    watch.Start();
                    PollResponse response = await queuesClient.Poll(pollRequest);

                    if (response.HasMessages)
                    {
                        response.AckAll();
                    }
                    
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
                finally
                {
                    watch.Stop();
                   
                }
                time= watch.ElapsedMilliseconds;
            });
            return time;
    }
       
        static async Task Main(string[] args)

        {
            

            var counter = 0;
            var roundsDone = false;
            do
            {
                counter++;
                if (rounds > 0 && counter >= rounds)
                {
                    roundsDone = true;
                }
                else
                {
                    roundsDone = false;
                }
                List<Message> msgs = new List<Message>();
                 for (int i = 0; i < tasks; i++)
                {
                    var channel = $"{queue}.{i+1}";
                    for (int t = 0; t < send; t++)
                    {
                        msgs.Add(new Message()
                        {
                            MessageID = i.ToString(),
                            Queue =channel,
                            Body = KubeMQ.SDK.csharp.Tools.Converter.ToByteArray($"im Message {t}"),
                            Metadata = "some-metadata",
                            Tags = new Dictionary<string, string>()/* ("Action", $"Batch_{testGui}_{i}")*/ 
                            {
                                {"Action",$"Batch_{t}"}
                            }
                        });
                    }
                    
                }
                await  queuesClient.Send(new SendRequest(msgs));
                Console.WriteLine($"sending {send*tasks} messages completed");
                Task[] taskArray = new Task[tasks];
                for (int i = 0; i < taskArray.Length; i++)
                {
                    {
                        taskArray[i] = GetPollMessages(i);
                    }
                }
                await Task.WhenAll(taskArray);
                long total = 0;
                foreach (Task<long> task in taskArray)
                {
                     total+=task.Result;
                }                
                
                Console.WriteLine($"Receiveing {tasks*send} messages takes {total/tasks}" +  " milliseconds");
                Thread.Sleep(1000);
            } while (!roundsDone);
        }
    }
}