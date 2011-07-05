using System;
using System.Numerics;

namespace Cassandra.ComputeRingTokens
{
    public static class EntryPoint
    {
        public static void Main(string[] args)
        {
            if(args.Length == 0)
            {
                PrintHelp();
                return;
            }

            int nodesCount;
            if(!int.TryParse(args[0], out nodesCount))
                PrintHelp();

            PrintTokenRing(nodesCount);
        }

        private static void PrintTokenRing(int nodesCount)
        {
            for(int i = 0; i < nodesCount; i++)
            {
                var tokenIndex = i + 1;
                var bigInteger = new BigInteger(2);
                bigInteger = BigInteger.Pow(bigInteger, 127);
                bigInteger = tokenIndex * bigInteger / nodesCount;
                Console.WriteLine(string.Format("Node №{0}:\t\t{1}", tokenIndex, bigInteger));
            }
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Usage: ComputeRingTokens.exe <nodesCount>");
        }
    }
}