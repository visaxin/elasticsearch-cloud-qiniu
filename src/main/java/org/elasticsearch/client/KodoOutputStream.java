
package org.elasticsearch.client;

import org.elasticsearch.blobstore.KodoBlobStore;
import org.elasticsearch.blobstore.KodoBlobContainer;
import com.qiniu.common.QiniuException;
import com.qiniu.http.Client;
import com.qiniu.http.Response;
import com.qiniu.storage.model.ResumeBlockInfo;
import com.qiniu.util.Crc32;
import com.qiniu.util.StringMap;
import com.qiniu.util.StringUtils;
import com.qiniu.util.UrlSafeBase64;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

public class KodoOutputStream extends OutputStream {
    private Client client;
    private KodoBlobStore blobStore;
    private String key;
    private ByteArrayOutputStream buffer;
    private ByteSizeValue kodoBlockSize;
    private boolean closed;
    private String mime;
    private List<String> contexts;
    private String host = "http://up.qiniu.com";
    private long size;
    private String upToken;
    private static final ESLogger logger = ESLoggerFactory.getLogger(KodoBlobContainer.class.getName());

    public KodoOutputStream(KodoBlobStore blobStore, String key, int bufferSize, int retention) throws IOException {
        this.key = key;
        this.blobStore = blobStore;
        this.client = new Client();
        this.mime = "application/octet-stream";
        this.contexts = new ArrayList();
        this.kodoBlockSize = new ByteSizeValue(4L, ByteSizeUnit.MB);
        this.closed = false;
        this.buffer = new ByteArrayOutputStream();
        this.upToken = blobStore.getClient().generateUploadTokenWithRetention(this.blobStore.getBucket(), this.key, retention);
    }

    public void write(int b) throws IOException {
        this.buffer.write(b);
        ++this.size;
        if((long)this.buffer.size() == this.kodoBlockSize.bytes()) {
            try {
                this.doUpload();
            } catch (QiniuException e) {
                throw e;
            }
        }

    }

    public void write(byte[] b, int off, int len) throws IOException {
        if(b == null) {
            throw new NullPointerException();
        } else if(off >= 0 && off <= b.length && len >= 0 && off + len <= b.length && off + len >= 0) {
            if(len != 0) {
                this.size += (long)len;
                this.buffer.write(b, off, len);

                try {
                    this.doUpload();
                } catch (QiniuException e
                        ) {
                    throw e;
                }
            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    public void doUpload() throws IOException {
        if(this.buffer.size() < this.kodoBlockSize.bytesAsInt()) {
            this.close();
        } else {
            byte[] allBytes = this.buffer.toByteArray();
            this.buffer.reset();
            int processedPos = 0;

            final ArrayList blocks = new ArrayList<String>();
            for(; (long)(allBytes.length - processedPos) >= this.kodoBlockSize.bytes(); processedPos = (int)((long)processedPos + this.kodoBlockSize.bytes())) {
                int i = 0;
                ResumeBlockInfo b = null;
                byte[] block = Arrays.copyOfRange(allBytes, processedPos, (int)((long)processedPos + this.kodoBlockSize.bytes()));

                for(long crc = Crc32.bytes(block); i < this.blobStore.getNumberOfRetries(); ++i) {
                    b = this.makeBlock(block, this.kodoBlockSize.bytes());
                    if(b != null && crc == b.crc32) {
                        break;
                    }
                }

                if(i >= this.blobStore.getNumberOfRetries()) {
                    throw new IOException("Upload Block Crc Check failed too much times(Max=" + this.blobStore.getNumberOfRetries() + ")");
                }

                if(b == null) {
                    throw new IOException("ResumeBlockInfo is null, key is " + this.key);
                }

                blocks.add(b.ctx);
            }

            if(allBytes.length - processedPos > 0) {
                this.buffer.write(Arrays.copyOfRange(allBytes, processedPos, allBytes.length), 0, allBytes.length - processedPos);
            }

            if(blocks.size() > 0) {
                this.contexts.addAll(blocks);
            }

        }
    }

    public void close() throws IOException {
        if(!this.closed) {
            this.closed = true;

            try {
                if(this.buffer.size() > 0) {
                    ResumeBlockInfo b = this.makeBlock(this.buffer.toByteArray(), (long)this.buffer.size());
                    this.contexts.add(b.ctx);
                }

                this.makeFile();
            } catch (QiniuException e) {
                throw e;
            } finally {
                this.buffer = null;
                super.close();
            }

        }
    }

    private ResumeBlockInfo makeBlock(byte[] block, long blockSize) throws QiniuException {
        String url = this.host + "/mkblk/" + blockSize;
        Response response = this.post(url, block, 0, blockSize);
        return response.jsonToObject(ResumeBlockInfo.class);
    }

    private void makeFile() throws QiniuException {
        String url = this.fileUrl();
        String s = StringUtils.join(this.contexts, ",");
        this.post(url, StringUtils.utf8Bytes(s));
    }

    private String fileUrl() {
        String url = this.host + "/mkfile/" + this.size + "/mimeType/" + UrlSafeBase64.encodeToString(this.mime);
        StringBuilder b = new StringBuilder(url);
        b.append("/key/");
        b.append(UrlSafeBase64.encodeToString(this.key));
        logger.debug("upload file to kodo:" + this.key + " ==>>>> KodoServer");
        return b.toString();
    }

    private Response post(String url, byte[] data) throws QiniuException {
        return this.client.post(url, data, (new StringMap()).put("Authorization", "UpToken " + this.upToken));
    }

    private Response post(String url, byte[] data, int offset, long size) throws QiniuException {
        return this.client.post(url, data, offset, (int)size, (new StringMap()).put("Authorization", "UpToken " + this.upToken), "application/octet-stream");
    }

}
