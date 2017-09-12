//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.elasticsearch.plugin;

import org.elasticsearch.repositories.KodoRepository;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;

public class KodoPlugin extends Plugin {
    private final Settings settings;

    public KodoPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "Qiniu-Kodo";
    }

    public String description() {
        return "This is Qiniu-Kodo Repository Plugin!";
    }

    public void onModule(RepositoriesModule repositoriesModule) {
        repositoriesModule.registerRepository(KodoRepository.TYPE, KodoRepository.class, BlobStoreIndexShardRepository.class);
    }
}
