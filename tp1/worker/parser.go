package worker
import (
	"fmt"
	"../storage"
	"net"
	"bufio"
	"strconv"
	"strings"
)

func filterStrlist(list []string, toskip string) []string {
	var filtered []string
	for _, name := range list {
		if name != toskip {
			filtered = append(filtered, name)
		}
	}
	return filtered
}
func filterSpaces(list []string) []string {
	return filterStrlist(list, "")
}

func parseLSLine(line string) (string, storage.Node) {
	//fmt.Printf("parse_ls_line: %s\n", line)
	var node storage.Node
	node.Size, _ = strconv.Atoi(filterSpaces(strings.Split(line, " "))[4])
	if line[0] == 'd' {
		node.Type = storage.Dir
	} else if line[0] == 'l' {
		node.Type = storage.Link
	} else {
		node.Type = storage.File
	}
	node.Files = make(storage.NodeMap)
	parts := filterSpaces(strings.Split(line, " ") )
	name := parts[len(parts)-1]
	return name, node
}
// Procesa los resultados de un ls que recibio el escuchante.
// Partiendo desde path, agrega los nodos nuevos
// al árbol que arranca en root_dir y encola los directorios a procesar en
// queue.
func parseLS(path string, dataserver net.Listener) (storage.Node, []string) {
	node := storage.Node{storage.Dir, 0, path, make(storage.NodeMap)}
	queue := make([]string, 0)
	dconn, err := dataserver.Accept()
	if err != nil {
		// handle error
	}
	dconnbuf := bufio.NewScanner(dconn)
	i := 0
	for dconnbuf.Scan() {
		i = i +1
		if i == 1 {
			continue
		}
		_str, _node := parseLSLine(dconnbuf.Text())
		_node.Path = fmt.Sprintf("%s%s/",path,_str)  //TODO if not dir do not add /

		node.Files[_str] = &_node
		node.Size += _node.Size

		if _node.Type == storage.Dir {
			//fmt.Printf("Encolamos %s\n", _node.Path)
			queue = append(queue, _node.Path)
		}
		//fmt.Printf("Path=%s, _str = %s, node = %q\n", path, _str, _node)
		//fmt.Println(_node.nodetype, _str, _node.size)


	}
	return node, queue
}