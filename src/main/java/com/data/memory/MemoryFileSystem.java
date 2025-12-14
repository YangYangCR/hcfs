package com.data.memory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MemoryFileSystem extends FileSystem {

    private Path workingDir = new Path("/");
    private static final Map<Path, ByteArrayOutputStream> files = new HashMap<>();
    private static final Map<Path, FsPermission> permissions = new HashMap<>();
    private static final Map<Path, Boolean> directories = new HashMap<>();
    private URI uri;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        System.out.println("this is MemoryFileSystem initialize  " + this.toString());
        System.out.println("initialize schema name " + name.getScheme());
        this.uri = URI.create(name.getScheme() + ":///");
        System.out.println("uri = " + this.uri);
        System.out.println("workingDir = " + this.workingDir);
        // ⚠️ 用 qualified Path
        Path root = new Path("/").makeQualified(this.uri, this.workingDir);
        System.out.println("root = " + root);
        directories.put(root, true);
        this.uri = name;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public String getScheme() {
        return "memfs";
    }

    public MemoryFileSystem() {
        directories.put(new Path("/"), true); // 根目录
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        if (files.containsKey(f) && !overwrite) {
            throw new IOException("File already exists: " + f);
        }
        files.put(f, new ByteArrayOutputStream(bufferSize));
        permissions.put(f, permission != null ? permission : FsPermission.getDefault());
        System.out.println("create path is " + f);
        System.out.println("create files is" + files.toString());
        return new FSDataOutputStream(files.get(f), statistics);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        ByteArrayOutputStream baos = files.get(f);
        if (baos == null) {
            throw new FileNotFoundException(f.toString());
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        return new FSDataInputStream(new BufferedFSInputStream(new FSInputStream() {
            @Override
            public void seek(long pos) throws IOException {
                bais.reset();
                bais.skip(pos);
            }

            @Override
            public long getPos() throws IOException {
                return baos.size() - bais.available();
            }

            @Override
            public boolean seekToNewSource(long targetPos) {
                return false;
            }

            @Override
            public int read() throws IOException {
                return bais.read();
            }
        }, bufferSize));
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        ByteArrayOutputStream baos = files.get(f);
        if (baos == null) throw new FileNotFoundException(f.toString());
        return new FSDataOutputStream(baos, statistics);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        if (files.containsKey(src)) {
            files.put(dst, files.remove(src));
            permissions.put(dst, permissions.remove(src));
            return true;
        }
        if (directories.containsKey(src)) {
            directories.put(dst, directories.remove(src));
            return true;
        }
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        if (files.containsKey(f)) {
            files.remove(f);
            permissions.remove(f);
            return true;
        }
        if (directories.containsKey(f)) {
            if (!recursive) return false;
            directories.remove(f);
            return true;
        }
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException {
        if (!directories.containsKey(f)) throw new FileNotFoundException(f.toString());
        System.out.println("list status " + f.toString());
        System.out.println("files is" + files.toString());
        return files.keySet().stream().filter(p -> p.getParent().equals(f)).map(p -> {
            try {
                return new FileStatus(files.get(p).size(), false, 1, 4096, System.currentTimeMillis(), p);
            } catch (Exception e) {
                return null;
            }
        }).filter(fs -> fs != null).toArray(FileStatus[]::new);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        workingDir = new_dir;
        directories.putIfAbsent(new_dir, true);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) {
        directories.put(f, true);
        permissions.put(f, permission != null ? permission : FsPermission.getDefault());
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws FileNotFoundException {
        if (files.containsKey(f)) {
            return new FileStatus(files.get(f).size(), false, 1, 4096, System.currentTimeMillis(), f);
        } else if (directories.containsKey(f)) {
            return new FileStatus(0, true, 1, 4096, System.currentTimeMillis(), f);
        }
        throw new FileNotFoundException(f.toString());
    }

    private Path qualify(Path p) {
        return p.makeQualified(uri, workingDir);
    }

}