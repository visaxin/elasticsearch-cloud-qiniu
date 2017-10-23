
package org.elasticsearch.repositories;

import org.elasticsearch.blobstore.KodoBlobStore;
import org.elasticsearch.client.KodoClient;
import org.elasticsearch.client.KodoService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.IndexShardRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;


public class KodoRepository extends BlobStoreRepository {
    public static final String TYPE = "kodo";
    private final KodoBlobStore blobStore;
    private final BlobPath basePath;
    private static final ESLogger log = ESLoggerFactory.getLogger(KodoRepository.class.getName());

    @Inject
    public KodoRepository(RepositoryName repositoryName, RepositorySettings settings, IndexShardRepository indexShardRepository) {
        super(repositoryName.name(), settings, indexShardRepository);

        String domain = settings.settings().get(KodoService.Repository_Kodo.BUCKET_DOMAIN);

        if (!domain.startsWith("http://")){
            throw new RepositoryException(repositoryName.name(), "Not a valid bucket domain defined! Only support http protocol.");
        }

        String bucket = settings.settings().get(KodoService.Repository_Kodo.BUCKET);
        if (null == bucket) {
            throw new RepositoryException(repositoryName.name(), "No bucket name defined!");
        } else {
            KodoClient kodoClient = new KodoClient(
                    settings.settings().get(KodoService.Repository_Kodo.ACCESS_KEY),
                    settings.settings().get(KodoService.Repository_Kodo.SECRET_KEY),
                    domain);

            String pathParam = settings.settings().get(KodoService.Repository_Kodo.BASE_PATH);
            pathParam = Strings.trimLeadingCharacter(pathParam, '/');
            if (!pathParam.endsWith("/")) {
                pathParam = pathParam + "/";
            }

            this.blobStore = new KodoBlobStore(settings.settings(), kodoClient, bucket, "", new ByteSizeValue(4L, ByteSizeUnit.MB), 3, pathParam);
            this.basePath = BlobPath.cleanPath();
        }
    }

    protected BlobStore blobStore() {
        return this.blobStore;
    }

    protected BlobPath basePath() {
        return this.basePath;
    }
}
