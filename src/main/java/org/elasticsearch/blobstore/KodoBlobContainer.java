//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.elasticsearch.blobstore;

import org.elasticsearch.client.KodoOutputStream;
import com.qiniu.common.QiniuException;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

public class KodoBlobContainer extends AbstractBlobContainer {
    private String keyPath;
    private final KodoBlobStore blobStore;
    private static final ESLogger logger = ESLoggerFactory.getLogger(KodoBlobContainer.class.getName());

    KodoBlobContainer(BlobPath path, KodoBlobStore blobStore, String keyPath) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = keyPath + path.buildAsString("/");
    }

    public boolean blobExists(String blobName) {
        try {
            logger.debug("blob exist {}", blobName);
            this.blobStore.getClient().stat(this.blobStore.getBucket(), this.buildKey(blobName));
            return true;
        } catch (QiniuException e) {
            if(e.code() == 612) {
                logger.debug(String.format("Not found %s", this.buildKey(blobName)));
            } else {
                e.printStackTrace();
            }
            return false;
        } catch (Throwable e) {
            throw new BlobStoreException("failed to check if blob exists", e);
        }
    }

    public InputStream readBlob(String blobName) throws IOException {
        return this.blobStore.getClient().getObject(this.buildKey(blobName));
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        try {
            logger.debug("write blob {}", blobName);
            OutputStream outputStream = new KodoOutputStream(this.blobStore, this.buildKey(blobName), this.blobStore.getBufferSize().bytesAsInt(), 1);
            Streams.copy(inputStream, outputStream);
        } catch (QiniuException e) {
            throw new BlobStoreException("cannot write data to output stream");
        }
    }

    public void writeBlob(String blobName, BytesReference bytes) throws IOException {
        try {
            logger.debug("write blob {}", blobName);
            OutputStream outputStream = new KodoOutputStream(this.blobStore, this.buildKey(blobName), this.blobStore.getBufferSize().bytesAsInt(), 1);
            bytes.writeTo(outputStream);
        } catch (QiniuException e) {
            throw new BlobStoreException("cannot write data to output stream");
        }
    }

    public void deleteBlob(String blobName) throws IOException {
        try {
            logger.debug("delete blob {}", blobName);
            this.blobStore.getClient().delete(this.blobStore.getBucket(), this.buildKey(blobName));
        } catch (QiniuException e) {
            throw new BlobStoreException("delete blob exception", e);
        }
    }

    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return this.listBlobsByPrefix((String)null);
    }

    public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        MapBuilder<String, BlobMetaData> mapBuilder = MapBuilder.newMapBuilder();
        String marker = "";

        FileListing fileListing;
        do {
            fileListing = this.blobStore.getClient().listObjects(this.blobStore.getBucket(), this.buildKey(blobNamePrefix), marker, 20);
            marker = fileListing.marker;
            FileInfo[] fileInfos = fileListing.items;
            int length = fileInfos.length;

            for(int i = 0; i < length; ++i) {
                FileInfo fileInfo = fileInfos[i];
                String name = fileInfo.key.substring(this.keyPath.length());
                mapBuilder.put(name, new PlainBlobMetaData(name, fileInfo.fsize));
            }
        } while(!fileListing.isEOF());

        return mapBuilder.immutableMap();
    }

    public void move(String sourceBlobName, String targetBlobName) throws IOException {
        this.blobStore.getClient().move(this.blobStore.getBucket(), this.buildKey(sourceBlobName), this.buildKey(targetBlobName));
    }

    private String buildKey(String blobName) {
        logger.debug("build key to  {}", this.keyPath + blobName);
        if(!this.keyPath.endsWith("/")) {
            this.keyPath = this.keyPath + "/";
        }

        return this.keyPath + blobName;
    }
}
