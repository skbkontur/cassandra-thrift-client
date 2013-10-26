using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;

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
            proc.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            proc.StartInfo.CreateNoWindow = true;
            proc.StartInfo.UseShellExecute = true;
            proc.Start();
        }

        private void Stop()
        {
            using(var mos = new ManagementObjectSearcher("SELECT ProcessId, CommandLine FROM Win32_Process"))
            {
                foreach(ManagementObject mo in mos.Get())
                {
                    if(mo["CommandLine"] != null && mo["CommandLine"].ToString().Contains("internalflag=kontur"))
                    {
                        Process.GetProcessById(int.Parse(mo["ProcessId"].ToString())).Kill();
                        Console.WriteLine("Killed process with id {0}", mo["ProcessId"]);
                    }
                }
            }
        }

        private void Deploy(string pathToCassandra, string deployPath)
        {
            if(Directory.Exists(deployPath))
                Directory.Delete(deployPath, true);
            Directory.CreateDirectory(deployPath);
            DirectoryCopy(pathToCassandra, deployPath, true);
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