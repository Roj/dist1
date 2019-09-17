package main
import (
	"strings"
	"strconv"
	"fmt"
)
const (
	dir = iota
	link
	file
)
//TODO: serialization
type Node struct {
	Type int
	Size int
	Path string // must have trailing /
	Files map[string]*Node
}
type NodeMap map[string]*Node

type Server struct {
	hostname string
	finished bool
	root_dir *Node
}

type ServerMap map[string]*Server

const (
	read = iota
	write
	newserver
	finishserver
)
type Query struct {
	Type int
	Hostname string
	Node Node
}

func filter_strlist(list []string, toskip string) []string {
	var filtered []string
	for _, name := range list {
		if name != toskip {
			filtered = append(filtered, name)
		}
	}
	return filtered
}
func filter_spaces(list []string) []string {
	return filter_strlist(list, "")
}

func parse_ls_line(line string) (string, Node) {
	fmt.Printf("parse_ls_line: %s\n", line)
	var node Node
	node.Size, _ = strconv.Atoi(filter_spaces(strings.Split(line, " "))[4])
	if line[0] == 'd' {
		node.Type = dir
	} else if line[0] == 'l' {
		node.Type = link
	} else {
		node.Type = file
	}
	node.Files = make(NodeMap)
	name := strings.Split(line, "\"")[1]
	return name, node
}
func get_subdir(path string, root_dir *Node) *Node {
	steps := strings.Split(path, "/")
	node := root_dir
	for _, subdir := range steps {
		if subdir == "" {
			continue
		}

		node = node.Files[subdir]
	}
	return node
}

func update_parents_size(path string, root_dir *Node, size int) {
	//TODO: factorizar usando funciones de orden superior
	node := root_dir
	node.Size = node.Size + size
	for _, subdir := range strings.Split(path, "/") {
		fmt.Printf("Escribiendo nodo %s, bajando a subdir %s\n", node.Path, subdir)
		if subdir == "" {
			continue
		}
		node = node.Files[subdir]
		node.Size = node.Size + size
	}
}

//TODO: pretty printer con stack de niveles para debuguear
