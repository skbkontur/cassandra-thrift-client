using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SkbKontur.Cassandra.Local
{
    public static class LocalCassandraNodeManager
    {
        public static void Restart(this LocalCassandraNode node, TimeSpan? timeout = null)
        {
            Stop(node, timeout);
            Deploy(node);
            Start(node, timeout);
        }

        public static void Start(this LocalCassandraNode node, TimeSpan? timeout = null)
        {
            var localNodeName = LocalCassandraProcessManager.StartLocalCassandraProcess(node.DeployDirectory);
            if (localNodeName != node.LocalNodeName)
                throw new InvalidOperationException($"actual localNodeName ({localNodeName}) != LocalNodeName ({node.LocalNodeName})");
            LocalCassandraProcessManager.WaitForLocalCassandraPortsToOpen(node.RpcPort, node.CqlPort, timeout);
        }

        public static void Stop(this LocalCassandraNode node, TimeSpan? timeout = null)
        {
            LocalCassandraProcessManager.StopLocalCassandraProcess(node.LocalNodeName, timeout);
        }

        public static void Deploy(this LocalCassandraNode node)
        {
            CleanupCassandraDeployDirectory(node.DeployDirectory);
            DirectoryCopy(node.TemplateDirectory, node.DeployDirectory);
            ExpandSettingsTemplates(node);
        }

        private static void CleanupCassandraDeployDirectory(string cassandraDeployDirectory)
        {
            if (!Directory.Exists(cassandraDeployDirectory))
                return;

            foreach (var file in Directory.GetFiles(cassandraDeployDirectory))
                File.Delete(file);

            foreach (var dir in Directory.GetDirectories(cassandraDeployDirectory))
            {
                if (string.Equals(Path.GetFileName(dir), "logs", StringComparison.OrdinalIgnoreCase))
                    continue;
                Directory.Delete(dir, recursive: true);
            }
        }

        private static void ExpandSettingsTemplates(LocalCassandraNode node)
        {
            var templateFiles = new[]
                {
                    @"conf/cassandra.yaml",
                    @"bin/cassandra.bat"
                };
            foreach (var templateFile in templateFiles)
                ExpandSettingsTemplate(Path.Combine(node.DeployDirectory, templateFile), node);
        }

        private static void ExpandSettingsTemplate(string templateFilePath, LocalCassandraNode node)
        {
            ExpandSettingsTemplate(templateFilePath, new Dictionary<string, string>
                {
                    {"ClusterName", node.ClusterName},
                    {"LocalNodeName", node.LocalNodeName},
                    {"HeapSize", node.HeapSize},
                    {"RpcAddress", node.RpcAddress},
                    {"ListenAddress", node.ListenAddress},
                    {"SeedAddresses", string.Join(",", node.SeedAddresses)},
                    {"RpcPort", node.RpcPort.ToString()},
                    {"CqlPort", node.CqlPort.ToString()},
                    {"JmxPort", node.JmxPort.ToString()},
                    {"GossipPort", node.GossipPort.ToString()}
                });
        }

        private static void ExpandSettingsTemplate(string templateFilePath, Dictionary<string, string> values)
        {
            var template = File.ReadAllText(templateFilePath);
            var settings = values.Aggregate(template, (current, value) => current.Replace("{{" + value.Key + "}}", value.Value));
            File.WriteAllText(templateFilePath, settings);
        }

        private static void DirectoryCopy(string sourceDirectoryName, string destinationDirectoryName)
        {
            var sourceDirectoryInfo = new DirectoryInfo(sourceDirectoryName);
            if (!sourceDirectoryInfo.Exists)
                throw new InvalidOperationException($"Source directory does not exist or could not be found: {sourceDirectoryName}");

            if (!Directory.Exists(destinationDirectoryName))
                Directory.CreateDirectory(destinationDirectoryName);

            var files = sourceDirectoryInfo.GetFiles();
            foreach (var file in files)
                file.CopyTo(Path.Combine(destinationDirectoryName, file.Name), overwrite: false);

            var subDirectories = sourceDirectoryInfo.GetDirectories();
            foreach (var subDirectory in subDirectories)
                DirectoryCopy(subDirectory.FullName, Path.Combine(destinationDirectoryName, subDirectory.Name));
        }
    }
}