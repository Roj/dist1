package storage
import (
	"strings"

)
const (
	Dir = iota
	Link
	File
)
type Node struct {
	Type int
	Size int
	Path string // must have trailing /
	Files map[string]*Node
}
type NodeMap map[string]*Node
type Server struct {
	Hostname string
	Finished bool
	Root_dir *Node
}

type ServerMap map[string]*Server
func (node Node) StrNodeType() string {
	if node.Type == Dir {
		return "dir"
	} else if node.Type == Link {
		return "link"
	} else {
		return "file"
	}
}
// Devuelve el nodo del subdirectorio pedido. Si no
// se encontro se devuelve nil.
func (root_dir *Node) GetSubdir(path string) *Node {
	steps := strings.Split(path, "/")
	node := root_dir
	for _, subdir := range steps {
		if subdir == "" {
			continue
		}

		_node, ok := node.Files[subdir]
		if ! ok {
			return nil
		}
		node = _node
	}
	return node
}

func (root_dir *Node) updateParentsSize(path string, size int) {
	//TODO: factorizar usando funciones de orden superior
	node := root_dir
	node.Size = node.Size + size
	for _, subdir := range strings.Split(path, "/") {
		//fmt.Printf("Escribiendo nodo %s, bajando a subdir %s\n", node.Path, subdir)
		if subdir == "" {
			continue
		}
		node = node.Files[subdir]
		node.Size = node.Size + size
	}
}
func (dict ServerMap) AddDir(server string, node Node) {

	// It always exists
	dbnode := dict[server].Root_dir.GetSubdir(node.Path)
	/*encoded, _ := json.Marshal(node)
	fmt.Printf("add_dir a escribir: %s\n", encoded)
	encoded, _ = json.Marshal(node)
	fmt.Printf("add_dir -- en la DB: %s\n", encoded)*/
	dbnode.Files = node.Files
	dict[server].Root_dir.updateParentsSize(node.Path, node.Size)
}
func (n Node) ShallowCopy() Node {
	shallow := Node{n.Type, n.Size, n.Path, make(NodeMap)}
	for k, v := range n.Files {
		shallow.Files[k] = &Node{v.Type, v.Size, v.Path, make(NodeMap)}
	}
	return shallow
}

func (dict ServerMap) GetDir(host string, path string) ResultsResponse {
	var response ResultsResponse
	if server, ok := dict[host]; ok {
		response.Finished = server.Finished

		node := server.Root_dir.GetSubdir(path)
		if node != nil {
			shallow_node := (*node).ShallowCopy()
			response.Node = shallow_node
		} else {
			response.Node = Node{File, 0, "/", make(NodeMap)}
		}

	} else {
		response.Finished = false
		response.Node = Node{File, -1, "/", make(NodeMap)}
	}
	return response
}