using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Threading;

namespace SKBKontur.Cassandra.CassandraDeploymentManager
{
    public class EntryPoint
    {
        public static void Main(string[] args)
        {
            var entryPoint = new EntryPoint();
            entryPoint.Run(args[0], args[1], args.Skip(2).ToArray());
        }

        private void Run(string pathToCassandra, string action, string[] additionalArgs)
        {
            if(action == "Deploy")
                Deploy(pathToCassandra, additionalArgs[0]);
            if(action == "Stop")
                Stop();
            if(action == "Start")
                Start(pathToCassandra);
            if(action == "FullRedeploy")
            {
                var deployedCassandraPath = additionalArgs[0];
                Stop();
                Deploy(pathToCassandra, deployedCassandraPath);
                Start(deployedCassandraPath);
            }
        }

        private void Start(string pathToCassandra)
        {
            var proc = new Process();
            proc.StartInfo.FileName = Path.Combine(pathToCassandra, @"bin\cassandra.bat");
            proc.StartInfo.WorkingDirectory = Path.Combine(pathToCassandra, @"bin");
            proc.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            proc.StartInfo.CreateNoWindow = true;
            proc.StartInfo.UseShellExecute = true;
            proc.Start();
        }

        private IEnumerable<int> GetCassandraProcessIds()
        {
            using(var mos = new ManagementObjectSearcher("SELECT ProcessId, CommandLine FROM Win32_Process"))
            {
                foreach(ManagementObject mo in mos.Get())
                {
                    if(mo["CommandLine"] != null && mo["CommandLine"].ToString().Contains("internalflag=kontur"))
                        yield return int.Parse(mo["ProcessId"].ToString());
                }
            }
        }

        private void Stop()
        {
            foreach(var processId in GetCassandraProcessIds())
            {
                Process.GetProcessById(processId).Kill();
                Console.WriteLine("Kill cassandra process id={0}", processId);
            }

            Console.WriteLine("Start waiting for cassandra process stop.");
            while(GetCassandraProcessIds().Any())
            {
                Console.WriteLine("Waiting for cassandra process stop.");
                Thread.Sleep(1000);
            }
                
        }

        private void Deploy(string pathToCassandra, string deployPath)
        {
            if(Directory.Exists(deployPath))
            {
                Directory.Delete(deployPath, true);
                Console.WriteLine("Deleted existed directory {0}.", deployPath);
            }
                
            Directory.CreateDirectory(deployPath);
            Console.WriteLine("Deploying cassandra to {0}.", deployPath);
            DirectoryCopy(pathToCassandra, deployPath, true);
            Console.WriteLine("Cassandra deployed to {0}.", deployPath);
        }

        private static void DirectoryCopy(string sourceDirName, string destDirName, bool copySubDirs)
        {
            // Get the subdirectories for the specified directory.
            var dir = new DirectoryInfo(sourceDirName);
            var dirs = dir.GetDirectories();

            if(!dir.Exists)
            {
                throw new DirectoryNotFoundException(
                    "Source directory does not exist or could not be found: "
                    + sourceDirName);
            }

            // If the destination directory doesn't exist, create it. 
            if(!Directory.Exists(destDirName))
                Directory.CreateDirectory(destDirName);

            // Get the files in the directory and copy them to the new location.
            var files = dir.GetFiles();
            foreach(var file in files)
            {
                var temppath = Path.Combine(destDirName, file.Name);
                file.CopyTo(temppath, false);
            }

            // If copying subdirectories, copy them and their contents to new location. 
            if(copySubDirs)
            {
                foreach(var subdir in dirs)
                {
                    var temppath = Path.Combine(destDirName, subdir.Name);
                    DirectoryCopy(subdir.FullName, temppath, copySubDirs);
                }
            }
        }
    }
}