/**
 * Copyright 2012 Shopzilla.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  http://tech.shopzilla.com
 *
 */

package com.shopzilla.hadoop.mapreduce;

import com.google.common.base.Function;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Jeremy Lucas
 * @since 6/8/12
 */
public class MiniMRClusterContext {
    private Configuration configuration = new Configuration();
    private MiniDFSCluster miniDFSCluster;
    private MiniMRCluster miniMrCluster;
    private Path hdfsRoot;
    private Resource localRoot;
    private Resource logDirectory = new FileSystemResource("/tmp/minimrcluster/logs");
    private File projectDirectory;
    private File buildDirectory;
    /*private TServer hiveServer;
    private HiveClient hiveClient;*/
    private PigServer pigServer;

    @PostConstruct
    public void start() {
        try {
            this.hdfsRoot = new Path(localRoot.getFile().getName());

            System.setProperty("hadoop.log.dir", logDirectory.getFilename());
            System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");

            miniDFSCluster = new MiniDFSCluster(configuration, 2, true, null);
            miniMrCluster = new MiniMRCluster(2, miniDFSCluster.getFileSystem().getUri().toString(), 1);

            File confFile = new File("/tmp/hadoop-site.xml");

            configuration.setInt("mapred.submit.replication", 1);
            configuration.set("dfs.datanode.address", "0.0.0.0:0");
            configuration.set("dfs.datanode.http.address", "0.0.0.0:0");
            configuration.writeXml(new FileOutputStream(confFile));

            System.setProperty("cluster", configuration.get("mapred.job.tracker"));
            System.setProperty("namenode", configuration.get("fs.default.name"));
            System.setProperty("junit.hadoop.conf", confFile.getPath());

            pigServer = new PigServer(ExecType.MAPREDUCE, ConfigurationUtil.toProperties(configuration));
            /*hiveServer = createHiveServer();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    hiveServer.serve();
                }
            }).start();

            hiveClient = createHiveClient();*/

            buildDirectory = new File(miniDFSCluster.getDataDirectory()).getParentFile().getParentFile().getParentFile().getParentFile();
            projectDirectory = buildDirectory.getParentFile();

            importHDFSDirectory(localRoot.getFile());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void importHDFSDirectory(final File file) throws Exception {
        Path path = new Path(hdfsRoot, "/" + localRoot.getFile().toURI().relativize(file.toURI()).getPath());
        if (file.isDirectory()) {
            miniDFSCluster.getFileSystem().mkdirs(path);
            miniDFSCluster.getFileSystem().makeQualified(path);
            for (File child : file.listFiles()) {
                importHDFSDirectory(child);
            }
        }
        else {
            miniDFSCluster.getFileSystem().copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), path);
            miniDFSCluster.getFileSystem().makeQualified(path);
        }
    }

    public FileSystem getFileSystem() {
        try {
            return miniDFSCluster.getFileSystem();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public PigServer getPigServer() {
        return pigServer;
    }


    public void processPaths(final Path path, final Function<Path, Void> pathProcessor) throws IOException {
        if (miniDFSCluster.getFileSystem().exists(path)) {
            FileStatus[] fileStatuses = miniDFSCluster.getFileSystem().listStatus(path);
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().toUri().getPath().startsWith("_")) {
                    pathProcessor.apply(fileStatus.getPath());
                }
            }
        }
        else {
            throw new IOException("Path does not exist: " + path);
        }
    }

    public void processPathsRecursive(final Path path, final Function<Path, Void> pathProcessor) throws IOException {
        if (miniDFSCluster.getFileSystem().exists(path)) {
            if (miniDFSCluster.getFileSystem().isFile(path)) {
                if (!path.toUri().getPath().startsWith("_")) {
                    pathProcessor.apply(path);
                }
            }
            else {
                FileStatus[] fileStatuses = miniDFSCluster.getFileSystem().listStatus(path);
                for (FileStatus fileStatus : fileStatuses) {
                    if (!fileStatus.getPath().toUri().getPath().startsWith("_")) {
                        processPathsRecursive(fileStatus.getPath(), pathProcessor);
                    }
                }
            }
        }
        else {
            throw new IOException("Path does not exist: " + path);
        }
    }

    public void processData(final Path path, final Function<String, Void> lineProcessor) throws IOException {
        final Function<Path, Void> pathProcessor = new Function<Path, Void>() {
            @Override
            public Void apply(Path path) {
                try {
                    FSDataInputStream in = miniDFSCluster.getFileSystem().open(path);
                    LineIterator lineIterator = new LineIterator(new InputStreamReader(in));
                    while (lineIterator.hasNext()) {
                        lineProcessor.apply(lineIterator.next());
                    }
                    lineIterator.close();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return null;
            }
        };
        processPaths(path, new Function<Path, Void>() {
            @Override
            public Void apply(Path input) {
                pathProcessor.apply(input);
                return null;
            }
        });
    }

    public void processDataRecursive(final Path path, final Function<String, Void> lineProcessor) throws IOException {
        final Function<Path, Void> pathProcessor = new Function<Path, Void>() {
            @Override
            public Void apply(Path path) {
                try {
                    FSDataInputStream in = miniDFSCluster.getFileSystem().open(path);
                    LineIterator lineIterator = new LineIterator(new InputStreamReader(in));
                    while (lineIterator.hasNext()) {
                        lineProcessor.apply(lineIterator.next());
                    }
                    lineIterator.close();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return null;
            }
        };
        processPathsRecursive(path, new Function<Path, Void>() {
            @Override
            public Void apply(Path input) {
                pathProcessor.apply(input);
                return null;
            }
        });
    }

    @PreDestroy
    public void stop() {
        try {
            Thread shutdownThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (pigServer != null) {
                            pigServer.shutdown();
                        }
                        if (miniDFSCluster != null) {
                            miniDFSCluster.shutdown();
                            miniDFSCluster = null;
                        }
                        if (miniMrCluster != null) {
                            miniMrCluster.shutdown();
                            miniMrCluster = null;
                        }
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            shutdownThread.start();
            shutdownThread.join(10000);
            FileUtils.deleteDirectory(logDirectory.getFile());
            FileUtils.deleteDirectory(buildDirectory);
            FileUtils.deleteDirectory(new File(projectDirectory, "logs"));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Required
    public void setConfiguration(final Configuration configuration) {
        this.configuration = configuration;
    }

    @Required
    public void setLocalRoot(final Resource localRoot) {
        this.localRoot = localRoot;
    }

    public void setLogDirectory(final Resource logDirectory) {
        this.logDirectory = logDirectory;
    }
}
