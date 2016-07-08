using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Lucene.Net.Store.Azure
{
    public class AzureDirectory : Directory
    {
        private readonly string _containerName;
        private readonly string _rootFolder;
        private CloudBlobClient _blobClient;


        /// <summary>
        /// Create an AzureDirectory
        /// </summary>
        /// <param name="storageAccount">storage account to use</param>
        /// <param name="containerName">name of container (folder in blob storage)</param>
        /// <param name="cacheDirectory">local Directory object to use for local cache</param>
        /// <param name="compressBlobs"></param>
        /// <param name="rootFolder">path of the root folder inside the container</param>
        public AzureDirectory(
            CloudStorageAccount storageAccount,
            string containerName = null,
            Directory cacheDirectory = null,
            bool compressBlobs = false,
            string rootFolder = null)
        {
            if (storageAccount == null)
                throw new ArgumentNullException("storageaccount");

            _containerName = string.IsNullOrEmpty(containerName) ? "lucene" : containerName.ToLower();


            if (string.IsNullOrEmpty(rootFolder))
                _rootFolder = string.Empty;
            else
            {
                rootFolder = rootFolder.Trim('/');
                _rootFolder = rootFolder + "/";
            }


            _blobClient = storageAccount.CreateCloudBlobClient();
            _initCacheDirectory(cacheDirectory);
            this.CompressBlobs = compressBlobs;
        }

        public CloudBlobContainer BlobContainer { get; private set; }

        public bool CompressBlobs
        {
            get;
            set;
        }

        public void ClearCache()
        {
            foreach (var file in CacheDirectory.ListAll())
            {
                CacheDirectory.DeleteFile(file);
            }
        }

        public Directory CacheDirectory { get; set; }

        private void _initCacheDirectory(Directory cacheDirectory)
        {
            if (cacheDirectory != null)
            {
                // save it off
                CacheDirectory = cacheDirectory;
            }
            else
            {
                var cachePath = Path.Combine(Path.GetPathRoot(Environment.SystemDirectory), "azuredirectory");
                var azureDir = new DirectoryInfo(cachePath);
                if (!azureDir.Exists)
                    azureDir.Create();

                var catalogPath = Path.Combine(cachePath, _containerName);
                var catalogDir = new DirectoryInfo(catalogPath);
                if (!catalogDir.Exists)
                    catalogDir.Create();

                CacheDirectory = FSDirectory.Open(catalogPath);
            }

            CreateContainer();
        }

        public void CreateContainer()
        {
            BlobContainer = _blobClient.GetContainerReference(_containerName);
            BlobContainer.CreateIfNotExists();
        }

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override string[] ListAll()
        {
            var results = from blob in BlobContainer.ListBlobs(_rootFolder)
                          select blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.LastIndexOf('/') + 1);

            return results.ToArray();
        }

        /// <summary>Returns true if a file with the given name exists. </summary>
        public override bool FileExists(string name)
        {
            // this always comes from the server
            try
            {
                return BlobContainer.GetBlockBlobReference(_rootFolder + name).Exists();
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Returns the time the named file was last modified. </summary>
        public override long FileModified(string name)
        {
            // this always has to come from the server
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
                blob.FetchAttributes();
                return blob.Properties.LastModified.Value.UtcDateTime.ToFileTimeUtc();
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>Set the modified time of an existing file to now. </summary>
        public override void TouchFile(string name)
        {
            //BlobProperties props = _blobContainer.GetBlobProperties(_rootFolder + name);
            //_blobContainer.UpdateBlobMetadata(props);
            // I have no idea what the semantics of this should be...hmmmm...
            // we never seem to get called
            CacheDirectory.TouchFile(name);
            //SetCachedBlobProperties(props);
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name)
        {
            var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
            blob.DeleteIfExists();
            Debug.WriteLine("DELETE {0}/{1}", BlobContainer.Uri, name);

            if (CacheDirectory.FileExists(name + ".blob"))
            {
                CacheDirectory.DeleteFile(name + ".blob");
            }

            if (CacheDirectory.FileExists(name))
            {
                CacheDirectory.DeleteFile(name);
            }
        }


        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name)
        {
            var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
            blob.FetchAttributes();

            // index files may be compressed so the actual length is stored in metatdata
            string blobLegthMetadata;
            var hasMetadataValue = blob.Metadata.TryGetValue("CachedLength", out blobLegthMetadata);

            long blobLength;
            if (hasMetadataValue && long.TryParse(blobLegthMetadata, out blobLength))
            {
                return blobLength;
            }
            return blob.Properties.Length; // fall back to actual blob size
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(string name)
        {
            var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
            return new AzureIndexOutput(this, blob);
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput OpenInput(string name)
        {
            try
            {
                var blob = BlobContainer.GetBlockBlobReference(_rootFolder + name);
                blob.FetchAttributes();
                return new AzureIndexInput(this, blob);
            }
            catch (Exception err)
            {
                throw new FileNotFoundException(name, err);
            }
        }

        private readonly Dictionary<string, AzureLock> _locks = new Dictionary<string, AzureLock>();

        /// <summary>Construct a {@link Lock}.</summary>
        /// <param name="name">the name of the lock file
        /// </param>
        public override Lock MakeLock(string name)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(name))
                {
                    _locks.Add(name, new AzureLock(_rootFolder + name, this));
                }
                return _locks[name];
            }
        }

        public override void ClearLock(string name)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(name))
                {
                    _locks[name].BreakLock();
                }
            }
            CacheDirectory.ClearLock(name);
        }

        /// <summary>Closes the store. </summary>
        protected override void Dispose(bool disposing)
        {
            BlobContainer = null;
            _blobClient = null;
        }

        public virtual bool ShouldCompressFile(string path)
        {
            if (!CompressBlobs)
                return false;

            var ext = Path.GetExtension(path);
            switch (ext)
            {
                case ".cfs":
                case ".fdt":
                case ".fdx":
                case ".frq":
                case ".tis":
                case ".tii":
                case ".nrm":
                case ".tvx":
                case ".tvd":
                case ".tvf":
                case ".prx":
                    return true;
                default:
                    return false;
            };
        }
        public StreamInput OpenCachedInputAsStream(string name)
        {
            return new StreamInput(CacheDirectory.OpenInput(name));
        }

        public StreamOutput CreateCachedOutputAsStream(string name)
        {
            return new StreamOutput(CacheDirectory.CreateOutput(name));
        }

    }

}
