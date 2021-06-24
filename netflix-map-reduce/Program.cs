using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace netflix_map_reduce
{
    class Program
    {
        static void Main(string[] args)
        {

            var sw = System.Diagnostics.Stopwatch.StartNew();

            string infile = AppDomain.CurrentDomain.BaseDirectory + @"\..\..\..\ratings.txt";

            sw.Restart();

            Dictionary<int, int> ReviewsByUser = new Dictionary<int, int>();

            Parallel.ForEach(

                //Data Structure to iterate
                File.ReadLines(infile),

                //TLS Initializer
                () => { return new Dictionary<int, int>(); },
                
                //Map
                (line, loopControl, localD) =>
                {
                    int userid = parse(line);
                    if (!localD.ContainsKey(userid))
                    {
                        localD.Add(userid, 1);
                    } else
                    {
                        localD[userid]++;
                    }

                    return localD;
                },

                //Reduce
                (mapResult) =>
                {
                    lock (ReviewsByUser)
                    {
                        foreach (int userid in mapResult.Keys)
                        {
                            int numreviews = mapResult[userid];
                            if (!ReviewsByUser.ContainsKey(userid))
                            {
                                ReviewsByUser.Add(userid, numreviews);
                            }
                            else
                            {
                                ReviewsByUser[userid] += numreviews;
                            }
                        }
                    }
                }

            );

            var top10 = ReviewsByUser.OrderByDescending(x => x.Value).Take(10);

            long timems = sw.ElapsedMilliseconds;

            Console.WriteLine();
            Console.WriteLine("** Top 10 users reviewing movies:");

            foreach (var user in top10)
            {
                Console.WriteLine("{0}: {1}", user.Key, user.Value);
            }

            Console.WriteLine();
            Console.WriteLine("** Done! Time: {0:0.00} secs", (timems / 1000));

            Console.ReadKey();
        }

        /// <summary>
		/// Parses one line of the netflix data file, and returns the userid who reviewed the movie.
		/// </summary>
		/// <param name="line"></param>
		/// <returns></returns>
		private static int parse(string line)
        {
            char[] separators = { ',' };

            string[] tokens = line.Split(separators);

            int movieid = Convert.ToInt32(tokens[0]);
            int userid = Convert.ToInt32(tokens[1]);
            int rating = Convert.ToInt32(tokens[2]);
            DateTime date = Convert.ToDateTime(tokens[3]);

            return userid;
        }
    }
}
