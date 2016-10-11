package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"io/ioutil"
	"encoding/json"
)

var progName = filepath.Base(os.Args[0])

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", progName)
	fmt.Fprintf(os.Stderr, "  %s JsonConfFile MOUNTPOINT\n", progName)
	flag.PrintDefaults()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix(progName + ": ")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	path := flag.Arg(0)
	mountpoint := flag.Arg(1)
	if err := mount(path, mountpoint); err != nil {
		log.Fatal(err)
	}
}

func mount(path, mountpoint string) error {

	// Rough outline:
	// TODO - make conf format and parse it and dump it all in a struct
	// TODO - hook it all into the fuse FS interface
	// TODO - actually implement reading files from S3 w/ readahead and other stuff in conf

	rootFs, err := LoadFileSystem(path)
	if err != nil {
		return err
	}
	fmt.Println(rootFs)
	return nil


	/*
	archive, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer archive.Close()

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		archive: &archive.Reader,
	}
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
	*/
}

type FS struct {
	archive *zip.Reader
}

var _ fs.FS = (*FS)(nil)


func (f *FS) Root() (fs.Node, error) {
	// TODO - store root s3 virtual dir in the FS struct
	n := &Dir{
		archive: f.archive,
	}
	return n, nil
}

type Dir struct {
	archive *zip.Reader
	// nil for the root directory, which has no entry in the zip
	file *zip.File
}

var _ fs.Node = (*Dir)(nil)

func zipAttr(f *zip.File) fuse.Attr {
	return fuse.Attr{
		Size:   f.UncompressedSize64,
		Mode:   f.Mode(),
		Mtime:  f.ModTime(),
		Ctime:  f.ModTime(),
		Crtime: f.ModTime(),
	}
}

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	// TODO - just return the one for root here
	if d.file == nil {
		// root directory
		*attr = fuse.Attr{Mode: os.ModeDir | 0755}
		return nil
	}
	*attr = zipAttr(d.file)
	return nil
}

var _ = fs.NodeRequestLookuper(&Dir{})

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	// TODO - split path into parts, simply traverse from current dir onwards
	path := req.Name
	if d.file != nil {
		path = d.file.Name + path
	}
	for _, f := range d.archive.File {
		switch {
		case f.Name == path:
			child := &File{
				file: f,
			}
			return child, nil
		case f.Name[:len(f.Name)-1] == path && f.Name[len(f.Name)-1] == '/':
			child := &Dir{
				archive: d.archive,
				file:    f,
			}
			return child, nil
		}
	}
	return nil, fuse.ENOENT
}

type File struct {
	file *zip.File
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	// TODO - just return read only 444....
	*attr = zipAttr(f.file)
	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	r, err := f.file.Open()
	if err != nil {
		return nil, err
	}
	// individual entries inside a zip file are not seekable
	resp.Flags |= fuse.OpenNonSeekable
	return &FileHandle{r: r}, nil
}

type FileHandle struct {
	r io.ReadCloser
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.r.Close()
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// We don't actually enforce Offset to match where previous read
	// ended. Maybe we should, but that would mean'd we need to track
	// it. The kernel *should* do it for us, based on the
	// fuse.OpenNonSeekable flag.
	buf := make([]byte, req.Size)
	n, err := fh.r.Read(buf)
	resp.Data = buf[:n]
	return err
}

var _ = fs.HandleReadDirAller(&Dir{})

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// TODO - simply just hand back merged lists of files and directories
	prefix := ""
	if d.file != nil {
		prefix = d.file.Name
	}

	var res []fuse.Dirent
	for _, f := range d.archive.File {
		if !strings.HasPrefix(f.Name, prefix) {
			continue
		}
		name := f.Name[len(prefix):]
		if name == "" {
			// the dir itself, not a child
			continue
		}
		if strings.ContainsRune(name[:len(name)-1], '/') {
			// contains slash in the middle -> is in a deeper subdir
			continue
		}
		var de fuse.Dirent
		if name[len(name)-1] == '/' {
			// directory
			name = name[:len(name)-1]
			de.Type = fuse.DT_Dir
		}
		de.Name = name
		res = append(res, de)
	}
	return res, nil
}



type JsonFileNode struct {
	Inode int `json:inode`
	IsDir int `json:isdir`
	Name string `json:name`
	Parent int `json:parent`
	Url string `json:url`
}

type JsonFileNodes struct {
	Nodes []JsonFileNode
}

type S3File struct {
	Inum int
	Name string
	ParentDirInum int
	Url string
}

type S3VirtualDir struct {
	Inum int
	Name string
	ParentDirInum int
	ChildFiles []S3File
	ChildDirs []S3VirtualDir
}

func LoadFileSystem(jsonPath string) (S3VirtualDir, error) {
	bytes, err := ioutil.ReadFile(jsonPath)
	if err != nil {
		return S3VirtualDir{}, err
	}

	nodes := make([]JsonFileNode, 0)
	err = json.Unmarshal(bytes, &nodes)
	if err != nil {
		return S3VirtualDir{}, err
	}

	// build map of inum -> fileNodes, then build file tree starting from root node
	// extracting things from the map
	nodeMap := make(map[int]JsonFileNode)
	for _, node := range nodes {
		nodeMap[node.Inode] = node
	}

	root := nodeMap[0] // deal with error case when root isnt specified some other time
	return buildDirNode(root, nodeMap), nil
}

func buildDirNode(curNode JsonFileNode, nodeMap map[int]JsonFileNode) S3VirtualDir {
	childDirs := []S3VirtualDir{}
	childFiles := []S3File{}

	for _, node := range nodeMap {
		if node.Parent == curNode.Inode {
			if node.IsDir == 1 {
				childDirs = append(childDirs, buildDirNode(node, nodeMap))
			} else {
				childFiles = append(childFiles, S3File{
					Name: node.Name,
					Url: node.Url,
					ParentDirInum: curNode.Inode,
					Inum: node.Inode,
				})
			}
		}
	}

	return S3VirtualDir{
		Name: curNode.Name,
		Inum: curNode.Inode,
		ParentDirInum: curNode.Parent,
		ChildDirs: childDirs,
		ChildFiles: childFiles,
	}
}
