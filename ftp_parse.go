package main
import (
	"strings"
	"strconv"
)
const (
	dir = iota
	link
	file
)
type Node struct {
	nodetype int
	size int
	files map[string]*Node
}
type NodeMap map[string]*Node

func filter_spaces(list []string) []string {
	var filtered []string
	for _, name := range list {
		if name != "" {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

func parse_ls_line(line string) (string, Node) {
	var node Node
	node.size, _ = strconv.Atoi(filter_spaces(strings.Split(line, " "))[4])
	if line[0] == 'd' {
		node.nodetype = dir
	} else if line[0] == 'l' {
		node.nodetype = link
	} else {
		node.nodetype = file
	}
	node.files = make(NodeMap)
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

		node = node.files[subdir]
	}
	return node
}
