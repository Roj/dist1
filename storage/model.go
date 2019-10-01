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
func Str_nodetype(node Node) string {
	if node.Type == Dir {
		return "dir"
	} else if node.Type == Link {
		return "link"
	} else {
		return "file"
	}
}
// Devuelve el nodo del subdirectorio pedido. El segundo
// elemento es true si hubo exito, o false si no se encontro.
func Get_subdir(path string, root_dir *Node) (*Node, bool) {
	steps := strings.Split(path, "/")
	node := root_dir
	for _, subdir := range steps {
		if subdir == "" {
			continue
		}

		_node, ok := node.Files[subdir]
		if ! ok {
			return nil, false
		}
		node = _node
	}
	return node, true
}

func update_parents_size(path string, root_dir *Node, size int) {
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
func Add_dir(dict ServerMap, server string, node Node) {

	// It always exists
	dbnode, _ := Get_subdir(node.Path, dict[server].Root_dir)
	/*encoded, _ := json.Marshal(node)
	fmt.Printf("add_dir a escribir: %s\n", encoded)
	encoded, _ = json.Marshal(node)
	fmt.Printf("add_dir -- en la DB: %s\n", encoded)*/
	dbnode.Files = node.Files
	update_parents_size(node.Path, dict[server].Root_dir, node.Size)
}
func Shallow_copy(n Node) Node {
	shallow := Node{n.Type, n.Size, n.Path, make(NodeMap)}
	for k, v := range n.Files {
		shallow.Files[k] = &Node{v.Type, v.Size, v.Path, make(NodeMap)}
	}
	return shallow
}

func Get_dir(dict ServerMap, host string, path string) ResultsResponse {
	var response ResultsResponse
	if server, ok := dict[host]; ok {
		response.Finished = server.Finished
		response.Node = Node{File, 0, "/", make(NodeMap)}
		node, exists := Get_subdir(path, server.Root_dir)
		if exists {
			shallow_node := Shallow_copy(*node)
			response.Node = shallow_node
		}

	} else {
		response.Finished = false
		response.Node = Node{File, -1, "/", make(NodeMap)}
	}
	return response
}