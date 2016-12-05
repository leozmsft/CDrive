using Microsoft.WindowsAzure.Storage.Blob;

namespace CDrive
{
    public class BlobQuery
    {
        public string Prefix { get; set; }
        public int MaxResult { get; set; }
        public bool ShowDirectory { get; set; }
        public BlobListingDetails BlobListingDetails { get; set; }

        public BlobQuery()
        {
            this.MaxResult = -1;
            this.BlobListingDetails = BlobListingDetails.All;
        }
    }
}
