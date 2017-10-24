package org.elasticsearch.client;

/**
 * Created by $Jason.Zhang on 5/4/17.
 */

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.Batch;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;
import com.qiniu.util.StringMap;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;

public class KodoClient {
    private static final String KODO_DELETE_AFTER_DAYS = "deleteAfterDays";
    private Auth auth;
    private String bucketDomain;
    private BucketManager bucketManager;

    public KodoClient(String ak, String sk, String bucketDomain) {
        this.auth = Auth.create(ak, sk);
        this.bucketDomain = bucketDomain;
        this.bucketManager = new BucketManager(this.auth);
    }

    private String getDownloadUrl(String key) throws IOException {
        String encodedFileName = URLEncoder.encode(key, "utf-8");
        String url = String.format("%s/%s", this.bucketDomain, encodedFileName);
        url = this.auth.privateDownloadUrl(url, 3600L);
        return url;
    }

    String generateUploadTokenWithRetention(String bucket, String key, int retention) throws QiniuException {
        StringMap properties = new StringMap();
        properties.put("scope", String.format("%s:%s", bucket, key));
        if (retention > 0) {
            properties.put(KODO_DELETE_AFTER_DAYS, retention);
        }

        return this.auth.uploadToken(bucket, key, 3600L, properties);
    }

    public FileInfo stat(String bucket, String key) throws QiniuException {
        FileInfo fileInfo = this.bucketManager.stat(bucket, key);
        return fileInfo;
    }

    public void delete(String bucket, String key) throws QiniuException {
        this.bucketManager.delete(bucket, key);
    }

    public InputStream getObject(String key) throws IOException {
        URL fileUrl = new URL(this.getDownloadUrl(key));
        return fileUrl.openConnection().getInputStream();
    }

    public FileListing listObjects(String bucket, String prefix, String marker, int limit) throws QiniuException {
        return this.bucketManager.listFiles(bucket, prefix, marker, limit, "");
    }

    public void move(String bucket, String sourceKey, String targetKey) throws QiniuException {
        this.bucketManager.move(bucket, sourceKey, bucket, targetKey, true);
    }

    public void deleteObjects(String bucket, String[] keys) throws QiniuException {
        Batch deleteOp = (new Batch()).delete(bucket, keys);
        this.bucketManager.batch(deleteOp);
    }

}
