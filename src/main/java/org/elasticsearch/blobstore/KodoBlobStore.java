
package org.elasticsearch.blobstore;

import org.elasticsearch.client.KodoClient;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import java.io.IOException;
import java.util.ArrayList;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

public class KodoBlobStore extends AbstractComponent implements BlobStore {
    private final ByteSizeValue bufferSize;
    private final KodoClient client;
    private final String bucket;
    private final String path;
    private final int numberOfRetries;

    public KodoBlobStore(Settings settings, KodoClient client, String bucket, ByteSizeValue bufferSize, int numberOfRetries, String path) {
        super(settings);
        this.client = client;
        this.bucket = bucket;
        this.path = path;
        this.numberOfRetries = numberOfRetries;
        this.bufferSize = bufferSize == null?new ByteSizeValue(4L, ByteSizeUnit.MB):bufferSize;
    }

    public BlobContainer blobContainer(BlobPath path) {
        return new KodoBlobContainer(path, this, this.path);
    }

    public void delete(BlobPath path) throws IOException {
        ArrayList<String> keysToDelete = new ArrayList<String>();
        String marker = "";

        FileListing fileListing;
        do {
            fileListing = this.client.listObjects(this.bucket, this.path, marker, 20);
            marker = fileListing.marker;
            FileInfo[] infos = fileListing.items;

            for (FileInfo info : infos) {
                keysToDelete.add(info.key);
            }
        } while(!fileListing.isEOF());

        this.client.deleteObjects(this.bucket, keysToDelete.toArray(new String[keysToDelete.size()]));
    }

    public void close() {
    }

    public KodoClient getClient() {
        return this.client;
    }

    public String getBucket() {
        return this.bucket;
    }

    public int getNumberOfRetries() {
        return this.numberOfRetries;
    }

    public ByteSizeValue getBufferSize() {
        return this.bufferSize;
    }
}
