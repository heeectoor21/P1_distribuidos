package main

import (
	"bufio"
	"encoding/gob"
	"log"
	"net"
	"os"
	"os/exec"
	"practica1/com"
	"time"
)

// función readWorkerEndpoints
// recibe como parametro el nombre del fichero txt
// donde se encuentran las direcciones ip:puerto de los workers
// lee el fichero txt y guarda las direcciones ip:puerto en un vector

func readWorkerEndpoints(filename string) ([]string, error) {

	// abre el fichero txt

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// crea el vector e introduce las direcciones linea a linea

	var endpoints []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		if line != "" {
			endpoints = append(endpoints, line)
		}
	}

	return endpoints, scanner.Err()
}

// función startWorker
// recibe como parametros la dirección ip:puerto del worker y la ruta del ejecutable
// para los procesos workers
// la función se encarga de ejecutar mediante ssh el ejecutable

func startWorker(workerEndpoint string, workerPath string) {

	// solo interesa la ip de la dirección ip:puerto para realizar ssh

	host, _, err := net.SplitHostPort(workerEndpoint)
	com.CheckError(err)

	// construye el comando

	command := workerPath + " " + workerEndpoint

	// ejecuta el comando

	cmd := exec.Command("ssh", host, command)
	err = cmd.Start()
	com.CheckError(err)
}

// función connectToWorker
// recibe como parámetros la dirección ip:puerto
// la función se encarga de realizar la conexión con la máquina

func connectToWorker(workerEndpoint string) net.Conn {

	// se intenta la conexión 10 veces

	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", workerEndpoint)
		if err == nil {
			return conn
		}
		time.Sleep(100 * time.Millisecond)
	}

	// error en caso de no poder realizar la conexión

	log.Fatal("Unable to connect to worker: ", workerEndpoint)
	return nil
}

// envía la petición al worker remoto y recibe su respuesta
func requestWorker(workerEndpoint string, request com.Request) com.Reply {
	workerConn := connectToWorker(workerEndpoint)
	defer workerConn.Close()

	encoder := gob.NewEncoder(workerConn)
	err := encoder.Encode(&request)
	com.CheckError(err)

	var reply com.Reply
	decoder := gob.NewDecoder(workerConn)
	err = decoder.Decode(&reply)
	com.CheckError(err)

	return reply
}

// envía la respuesta del worker al cliente
func replyClient(clientConn net.Conn, reply com.Reply) {
	encoder := gob.NewEncoder(clientConn)
	err := encoder.Encode(&reply)
	com.CheckError(err)
}

// recibe la petición del cliente
func receiveRequest(clientConn net.Conn) com.Request {
	var request com.Request

	decoder := gob.NewDecoder(clientConn)
	err := decoder.Decode(&request)
	com.CheckError(err)

	return request
}

// función worker
// recibe parámetros el canal tasks y la dirección ip:puerto
// la función se encarga de la comunicación entre el servidor (maestro) y worker (esclavo)

func worker(tasks chan net.Conn, workerEndpoint string) {
	for clientConn := range tasks {
		request := receiveRequest(clientConn)
		reply := requestWorker(workerEndpoint, request)
		replyClient(clientConn, reply)
		clientConn.Close()
	}
}

func main() {

	// comprueba el número de argumentos
	// argumento 1: dirección ip:puerto
	// argumento 2: nombre del fichero txt donde se encuentran las direcciones ip:puerto de los workers
	// argumento 3: ruta del ejecutable para los procesos worker

	args := os.Args
	if len(args) != 4 {
		log.Println("Error: go run master.go ip:port workers_file worker_path")
		os.Exit(1)
	}
	endpoint := args[1]
	workersFile := args[2]
	workerPath := args[3]

	// crea listener

	listener, err := net.Listen("tcp", endpoint)

	com.CheckError(err)                              // comprueba si hay un error
	log.SetFlags(log.Lshortfile | log.Lmicroseconds) // configura como se muestra los log

	log.Println("***** Master listening in endpoint ", endpoint)

	// crea canal tasks
	// el canal tasks se usa para envíar la conexión con el cliente desde el
	// proceso main a las Gourutines worker

	tasks := make(chan net.Conn)

	// lee el fichero con direcciones ip:puerto y lo guarda en un vector

	workerEndpoints, err := readWorkerEndpoints(workersFile)
	com.CheckError(err)

	// arranca las máquinas remotas

	for _, workerEndpoint := range workerEndpoints {
		startWorker(workerEndpoint, workerPath)
	}

	// se lanzan varias Gourutines worker

	for _, workerEndpoint := range workerEndpoints {
		go worker(tasks, workerEndpoint)
	}

	// si se acepta una conexión se envía por el canal tasks a una Gourutine worker
	// la recibira el primero que este disponible

	for {
		conn, err := listener.Accept()
		com.CheckError(err)
		tasks <- conn
	}
}
