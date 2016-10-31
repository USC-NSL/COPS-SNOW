/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.File;
import java.io.FileFilter;
import java.io.IOError;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedSegmentedFile;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.Pair;

/**
 * Encapsulate handling of paths to the data files.
 *
 * The directory layout is the following:
 *   /<path_to_data_dir>/ks/cf1/ks-cf1-hb-1-Data.db
 *                         /cf2/ks-cf2-hb-1-Data.db
 *                         ...
 *
 * In addition, more that one 'root' data directory can be specified so that
 * <path_to_data_dir> potentially represents multiple locations.
 * Note that in the case of multiple locations, the manifest for the leveled
 * compaction is only in one of the location.
 *
 * Snapshots (resp. backups) are always created along the sstables thare are
 * snapshoted (resp. backuped) but inside a subdirectory named 'snapshots'
 * (resp. backups) (and snapshots are furter inside a subdirectory of the name
 * of the snapshot).
 *
 * This class abstracts all those details from the rest of the code.
 */
public class Directories
{
    private static Logger logger = LoggerFactory.getLogger(Directories.class);

    public static final String BACKUPS_SUBDIR = "backups";
    public static final String SNAPSHOT_SUBDIR = "snapshots";
    public static final char SECONDARY_INDEX_NAME_SEPARATOR = '.';

    public static final File[] dataFileLocations;
    static
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        dataFileLocations = new File[locations.length];
        for (int i = 0; i < locations.length; ++i)
            dataFileLocations[i] = new File(locations[i]);
    }

    private final String tablename;
    private final String cfname;
    private final File[] sstableDirectories;

    public static Directories create(String tablename, String cfname)
    {
        int idx = cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
        if (idx > 0)
            // secondary index, goes in the same directory than the base cf
            return new Directories(tablename, cfname, cfname.substring(0, idx));
        else
            return new Directories(tablename, cfname, cfname);
    }

    private Directories(String tablename, String cfname, String directoryName)
    {
        this.tablename = tablename;
        this.cfname = cfname;
        this.sstableDirectories = new File[dataFileLocations.length];
        for (int i = 0; i < dataFileLocations.length; ++i)
            sstableDirectories[i] = new File(dataFileLocations[i], join(tablename, directoryName));

        if (!StorageService.instance.isClientMode())
        {
            try
            {
                for (File dir : sstableDirectories)
                    FileUtils.createDirectory(dir);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    public File getDirectoryForNewSSTables(long estimatedSize)
    {
        File path = getLocationWithMaximumAvailableSpace(estimatedSize);
        // Requesting GC has a chance to free space only if we're using mmap and a non SUN jvm
        if (path == null
         && (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap || DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.mmap)
         && !MmappedSegmentedFile.isCleanerAvailable())
        {
            StorageService.instance.requestGC();
            // retry after GCing has forced unmap of compacted SSTables so they can be deleted
            // Note: GCInspector will do this already, but only sun JVM supports GCInspector so far
            SSTableDeletingTask.rescheduleFailedTasks();
            try
            {
                Thread.sleep(10000);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            path = getLocationWithMaximumAvailableSpace(estimatedSize);
        }
        return path;
    }

    /*
     * Loop through all the disks to see which disk has the max free space
     * return the disk with max free space for compactions. If the size of the expected
     * compacted file is greater than the max disk space available return null, we cannot
     * do compaction in this case.
     */
    public File getLocationWithMaximumAvailableSpace(long estimatedSize)
    {
        long maxFreeDisk = 0;
        File maxLocation = null;

        for (File dir : sstableDirectories)
        {
            if (maxFreeDisk < dir.getUsableSpace())
            {
                maxFreeDisk = dir.getUsableSpace();
                maxLocation = dir;
            }
        }
        logger.debug("expected data files size is {}; largest free partition has {} bytes free", estimatedSize, maxFreeDisk);

        // Load factor of 0.9 we do not want to use the entire disk that is too risky.
        maxFreeDisk = (long)(0.9 * maxFreeDisk);
        return estimatedSize < maxFreeDisk ? maxLocation : null;
    }

    public static File getSnapshotDirectory(Descriptor desc, String snapshotName)
    {
        return getOrCreate(desc.directory, SNAPSHOT_SUBDIR, snapshotName);
    }

    public static File getBackupsDirectory(Descriptor desc)
    {
        return getOrCreate(desc.directory, BACKUPS_SUBDIR);
    }

    public SSTableLister sstableLister()
    {
        return new SSTableLister();
    }

    public class SSTableLister
    {
        private boolean skipCompacted;
        private boolean skipTemporary;
        private boolean includeBackups;
        private int nbFiles;
        private final Map<Descriptor, Set<Component>> components = new HashMap<Descriptor, Set<Component>>();
        private boolean filtered;

        public SSTableLister skipCompacted(boolean b)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            skipCompacted = b;
            return this;
        }

        public SSTableLister skipTemporary(boolean b)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            skipTemporary = b;
            return this;
        }

        public SSTableLister includeBackups(boolean b)
        {
            if (filtered)
                throw new IllegalStateException("list() has already been called");
            includeBackups = b;
            return this;
        }

        public Map<Descriptor, Set<Component>> list()
        {
            filter();
            return ImmutableMap.copyOf(components);
        }

        public List<File> listFiles()
        {
            filter();
            List<File> l = new ArrayList<File>(nbFiles);
            for (Map.Entry<Descriptor, Set<Component>> entry : components.entrySet())
            {
                for (Component c : entry.getValue())
                {
                    l.add(new File(entry.getKey().filenameFor(c)));
                }
            }
            return l;
        }

        private void filter()
        {
            if (filtered)
                return;

            for (File location : sstableDirectories)
            {
                location.listFiles(getFilter());

                if (includeBackups)
                    new File(location, BACKUPS_SUBDIR).listFiles(getFilter());
            }
            filtered = true;
        }

        private FileFilter getFilter()
        {
            // Note: the prefix needs to include cfname + separator to distinguish between a cfs and it's secondary indexes
            final String sstablePrefix = tablename + Component.separator + cfname + Component.separator;
            return new FileFilter()
            {
                // This function always return false since accepts adds to the components map
                public boolean accept(File file)
                {
                    // we are only interested in the SSTable files that belong to the specific ColumnFamily
                    if (file.isDirectory() || !file.getName().startsWith(sstablePrefix))
                        return false;

                    Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file.getParentFile(), file.getName());
                    if (pair == null)
                        return false;

                    if (skipCompacted && new File(pair.left.filenameFor(Component.COMPACTED_MARKER)).exists())
                        return false;
                    if (skipTemporary && pair.left.temporary)
                        return false;

                    Set<Component> previous = components.get(pair.left);
                    if (previous == null)
                    {
                        previous = new HashSet<Component>();
                        components.put(pair.left, previous);
                    }
                    previous.add(pair.right);
                    nbFiles++;
                    return false;
                }
            };
        }
    }

    public File tryGetLeveledManifest()
    {
        for (File dir : sstableDirectories)
        {
            File manifestFile = new File(dir, cfname + LeveledManifest.EXTENSION);
            if (manifestFile.exists())
            {
                logger.debug("Found manifest at {}", manifestFile);
                return manifestFile;
            }
        }
        logger.debug("No level manifest found");
        return null;
    }

    public File getOrCreateLeveledManifest()
    {
        File manifestFile = tryGetLeveledManifest();
        if (manifestFile == null)
            manifestFile = new File(sstableDirectories[0], cfname + LeveledManifest.EXTENSION);
        return manifestFile;
    }

    public void snapshotLeveledManifest(String snapshotName) throws IOException
    {
        File manifest = tryGetLeveledManifest();
        if (manifest != null)
        {
            File snapshotDirectory = getOrCreate(manifest.getParentFile(), SNAPSHOT_SUBDIR, snapshotName);
            CLibrary.createHardLink(manifest, new File(snapshotDirectory, manifest.getName()));
        }
    }

    public boolean snapshotExists(String snapshotName)
    {
        for (File dir : sstableDirectories)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, snapshotName));
            if (snapshotDir.exists())
                return true;
        }
        return false;
    }

    public void clearSnapshot(String snapshotName) throws IOException
    {
        // If snapshotName is empty or null, we will delete the entire snapshot directory
        String tag = snapshotName == null ? "" : snapshotName;
        for (File dir : sstableDirectories)
        {
            File snapshotDir = new File(dir, join(SNAPSHOT_SUBDIR, tag));
            if (snapshotDir.exists())
            {
                if (logger.isDebugEnabled())
                    logger.debug("Removing snapshot directory " + snapshotDir);
                FileUtils.deleteRecursive(snapshotDir);
            }
        }
    }

    private static File getOrCreate(File base, String... subdirs)
    {
        File dir = subdirs == null || subdirs.length == 0 ? base : new File(base, join(subdirs));
        if (dir.exists())
        {
            if (!dir.isDirectory())
                throw new IOError(new IOException(String.format("Invalid directory path %s: path exists but is not a directory", dir)));
        }
        else if (!dir.mkdirs())
        {
            throw new IOError(new IOException("Unable to create directory " + dir));
        }
        return dir;
    }

    private static String join(String... s)
    {
        return StringUtils.join(s, File.separator);
    }

    /**
     * To check if sstables needs migration, we look at the System directory.
     * If it contains a directory for the status cf, we'll attempt a sstable
     * migration.
     * Note that it is mostly harmless to try a migration uselessly, except
     * maybe for some wasted cpu cycles.
     */
    public static boolean sstablesNeedsMigration()
    {
        if (StorageService.instance.isClientMode())
            return false;

        boolean hasSystemKeyspace = false;
        for (File location : dataFileLocations)
        {
            File systemDir = new File(location, Table.SYSTEM_TABLE);
            hasSystemKeyspace |= (systemDir.exists() && systemDir.isDirectory());
            File statusCFDir = new File(systemDir, SystemTable.STATUS_CF);
            if (statusCFDir.exists())
                return false;
        }
        if (!hasSystemKeyspace)
            // This is a brand new node.
            return false;

        // Check whether the migration migth create too long a filename
        int longestLocation = -1;
        try
        {
            for (File loc : dataFileLocations)
                longestLocation = Math.max(longestLocation, loc.getCanonicalPath().length());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        for (KSMetaData ksm : Schema.instance.getTableDefinitions())
        {
            String ksname = ksm.name;
            for (Map.Entry<String, CFMetaData> entry : ksm.cfMetaData().entrySet())
            {
                String cfname = entry.getKey();
                // max path is roughly (guess-estimate) <location>/ksname/cfname/snapshots/1324314347102-somename/ksname-cfname-tmp-hb-1024-Statistics.db
                if (longestLocation + (ksname.length() + cfname.length()) * 2 + 62 > 256)
                    throw new RuntimeException("Starting with 1.1, keyspace names and column family names must be less than 32 characters long. "
                        + ksname + "/" + cfname + " doesn't respect that restriction. Please rename your keyspace/column families to respect that restriction before updating.");
            }
        }
        return true;
    }

    /**
     * Move sstables from the pre-#2749 layout to their new location/names.
     * This involves:
     *   - moving each sstable to their CF specific directory
     *   - rename the sstable to include the keyspace in the filename
     *
     * Note that this also move leveled manifests, snapshots and backups.
     */
    public static void migrateSSTables()
    {
        logger.info("Upgrade from pre-1.1 version detected: migrating sstables to new directory layout");

        for (File location : dataFileLocations)
        {
            if (!location.exists() || !location.isDirectory())
                continue;

            for (File ksDir : location.listFiles())
            {
                if (!ksDir.isDirectory())
                    continue;

                for (File file : ksDir.listFiles())
                    migrateFile(file, ksDir, null);

                migrateSnapshots(ksDir);
                migrateBackups(ksDir);
            }
        }
    }

    private static void migrateSnapshots(File ksDir)
    {
        File snapshotDir = new File(ksDir, SNAPSHOT_SUBDIR);
        if (!snapshotDir.exists())
            return;

        for (File snapshot : snapshotDir.listFiles())
        {
            if (!snapshot.isDirectory())
                continue;

            for (File f : snapshot.listFiles())
                migrateFile(f, ksDir, join(SNAPSHOT_SUBDIR, snapshot.getName()));

            if (!snapshot.delete())
                logger.info("Old snapsot directory {} not deleted by migraation as it is not empty", snapshot);
        }
        if (!snapshotDir.delete())
            logger.info("Old directory {} not deleted by migration as it is not empty", snapshotDir);
    }

    private static void migrateBackups(File ksDir)
    {
        File backupDir = new File(ksDir, BACKUPS_SUBDIR);
        if (!backupDir.exists())
            return;

        for (File f : backupDir.listFiles())
            migrateFile(f, ksDir, BACKUPS_SUBDIR);

        if (!backupDir.delete())
            logger.info("Old directory {} not deleted by migration as it is not empty", backupDir);
    }

    private static void migrateFile(File file, File ksDir, String additionalPath)
    {
        try
        {
            if (file.isDirectory())
                return;

            String name = file.getName();
            boolean isManifest = name.endsWith(LeveledManifest.EXTENSION);
            String cfname = isManifest
                          ? name.substring(0, name.length() - LeveledManifest.EXTENSION.length())
                          : name.substring(0, name.indexOf(Component.separator));

            int idx = cfname.indexOf(SECONDARY_INDEX_NAME_SEPARATOR); // idx > 0 => secondary index
            String dirname = idx > 0 ? cfname.substring(0, idx) : cfname;
            File destDir = getOrCreate(ksDir, dirname, additionalPath);

            File destFile = new File(destDir, isManifest ? name : ksDir.getName() + Component.separator + name);
            logger.debug(String.format("[upgrade to 1.1] Moving %s to %s", file, destFile));
            FileUtils.renameWithConfirm(file, destFile);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    // Hack for tests, don't use otherwise
    static void overrideDataDirectoriesForTest(String loc)
    {
        for (int i = 0; i < dataFileLocations.length; ++i)
            dataFileLocations[i] = new File(loc);
    }

    // Hack for tests, don't use otherwise
    static void resetDataDirectoriesAfterTest()
    {
        String[] locations = DatabaseDescriptor.getAllDataFileLocations();
        for (int i = 0; i < locations.length; ++i)
            dataFileLocations[i] = new File(locations[i]);
    }
}
