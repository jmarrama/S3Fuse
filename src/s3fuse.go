package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"io/ioutil"
	"encoding/json"
	"net/http"
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
	defer println("ending")
	path := flag.Arg(0)
	mountpoint := flag.Arg(1)
	if err := mount(path, mountpoint); err != nil {
		log.Fatal(err)
	}
}

func mount(path, mountpoint string) error {

	// Rough outline:
	// TODO - actually implement reading files from S3 w/ readahead and other stuff in conf

	rootFs, err := LoadFileSystem(path)
	if err != nil {
		return err
	}
	//fmt.Println(rootFs)

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		rootNode: &rootFs,
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
}

type FS struct {
	rootNode *S3VirtualDir
}

var _ fs.FS = (*FS)(nil)


func (f *FS) Root() (fs.Node, error) {
	n := &Dir{
		node: f.rootNode,
	}
	return n, nil
}

type Dir struct {
	node *S3VirtualDir
}

var _ fs.Node = (*Dir)(nil)

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	// everything is read only in this FS
	*attr = fuse.Attr{Mode: os.ModeDir | 0555}
	return nil
}

var _ = fs.NodeRequestLookuper(&Dir{})

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	pathComps := strings.Split(req.Name, "/")
	isCurNodeFile := false
	curDir := d.node
	curFile := &S3File{}

	for _, curPathComp := range pathComps {

		if isCurNodeFile {
			return nil, fuse.ENOENT
		}

		found := false

		// TODO - migrate data model to put dirs and files under the same supertype, this is so ugly
		// search for curPathComp in this directory's children
		for _, cdir := range curDir.ChildDirs {
			if strings.Compare(cdir.Name, curPathComp) == 0 {
				curDir = &cdir
				found = true
				break
			}
		}
		for _, cfile := range curDir.ChildFiles {
			if strings.Compare(cfile.Name, curPathComp) == 0 {
				curFile = &cfile
				isCurNodeFile = true
				found = true
				break
			}
		}

		if found == false {
			return nil, fuse.ENOENT
		}
	}

	if isCurNodeFile {
		return &File{curFile}, nil
	}
	return &Dir{curDir}, nil
}

type File struct {
	node *S3File
}

var _ fs.Node = (*File)(nil)

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	*attr = fuse.Attr{Mode: 0444}
	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	httpResp, err := http.Get(f.node.Url)
	if err != nil {
		return nil, err
	}
	println("opened file %s with http resp %d", f.node.Name, httpResp.StatusCode)
	// lets deal with seek later?
	resp.Flags |= fuse.OpenNonSeekable
	return &FileHandle{
		resp: httpResp,
		pos: 0,
		buf: make([]byte, bufSize),
	}, nil
}

const bufSize = 1000000 // lets just hardcode size for now....

type FileHandle struct {
	resp *http.Response
	buf []byte
	pos uint64
}

var _ fs.Handle = (*FileHandle)(nil)

var _ fs.HandleReleaser = (*FileHandle)(nil)

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.resp.Body.Close()
}

var _ = fs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	//offset := req.Offset
	println("read was called")
	sizeRequested := req.Size

	if (sizeRequested > bufSize) {
		sizeRequested = bufSize
	}

	bytesRead, err := fh.resp.Body.Read(fh.buf)
	println("read %d bytes", bytesRead)
	resp.Data = fh.buf[:bytesRead]
	return err
}

var _ = fs.HandleReadDirAller(&Dir{})

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var res []fuse.Dirent

	for _, d := range d.node.ChildDirs {
		var de fuse.Dirent
		de.Type = fuse.DT_Dir
		de.Name = d.Name
		res = append(res, de)
	}
	for _, f := range d.node.ChildFiles {
		var fe fuse.Dirent
		fe.Type = fuse.DT_File
		fe.Name = f.Name
		res = append(res, fe)

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
