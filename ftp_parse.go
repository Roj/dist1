package main
import (
	"fmt"
	"strings"
	"io/ioutil"
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
	name := strings.Split(line, "\"")[1]
	return name, node
}
func main() {
	dat, _ := ioutil.ReadFile("res.txt")
	rawstr := string(dat)
	//fmt.Print(rawstr)
	lines := strings.Split(rawstr, "\n")
	root_dir := make(NodeMap)

	fmt.Printf("%q\n", lines[15])
	name, node := parse_ls_line(lines[15])
	root_dir[name] = &node

	fmt.Printf("%q\n", root_dir[name])
}