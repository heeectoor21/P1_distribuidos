package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// scanEndpoints lee del fichero las direcciones de todos los procesos de la
// barrera. Cada línea contiene un endpoint en formato host:puerto.
func scanEndpoints(file *os.File) ([]string, error) {
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

// readEndpoints lee del fichero las direcciones de todos los procesos de la
// barrera. Cada línea contiene un endpoint en formato host:puerto.
func readEndpoints(filename string) ([]string, error) {
	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	return scanEndpoints(file)
}

// registerNotification registra la llegada de un proceso remoto a la barrera.
// El mapa evita contar dos veces un mismo mensaje y el mutex protege el mapa
// porque esta función se ejecuta en una goroutine por cada conexión.
func registerNotification(
	msg string,
	received *map[string]bool,
	mu *sync.Mutex,
) int {
	mu.Lock()
	defer mu.Unlock()

	(*received)[msg] = true

	return len(*received)
}

// handleConnection registra la llegada de un proceso remoto a la barrera.
// El mapa evita contar dos veces un mismo mensaje y el mutex protege el mapa
// porque esta función se ejecuta en una goroutine por cada conexión.
func handleConnection(
	conn net.Conn,
	barrierChan chan<- bool,
	received *map[string]bool,
	mu *sync.Mutex,
	n int,
) {
	defer conn.Close()

	buf := make([]byte, 1024)
	bytesRead, err := conn.Read(buf)

	if err != nil {
		return
	}

	count := registerNotification(
		string(buf[:bytesRead]),
		received,
		mu,
	)

	fmt.Println("Received", count, "elements")

	if count == n-1 {
		barrierChan <- true
	}
}

// getEndpoints valida los argumentos del proceso y obtiene sus endpoints.
// lineNumber identifica la posición de este proceso dentro del fichero.
func getEndpoints() ([]string, int, error) {
	endpointsFile := os.Args[1]
	var endpoints []string
	lineNumber, err := strconv.Atoi(os.Args[2])
	if err != nil || lineNumber < 1 {
		return nil, 0, errors.New("invalid line number")
	} else if endpoints, err = readEndpoints(endpointsFile); err != nil {
		fmt.Println("Error reading endpoints:", err)
	} else if lineNumber > len(endpoints) {
		fmt.Printf("Line number %d out of range\n", lineNumber)
		err = errors.New("line number out of range")
	}
	return endpoints, lineNumber, err
}

// acceptAndHandleConnections acepta conexiones entrantes hasta que se recibe
// la orden de detener el listener, delegando cada conexión a una goroutine.
func acceptAndHandleConnections(
	listener net.Listener,
	quitChannel chan bool,
	barrierChan chan bool,
	receivedMap *map[string]bool,
	mu *sync.Mutex,
	n int,
) {
	for {
		select {
		case <-quitChannel:
			fmt.Println("Stopping the listener...")
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				return
			}
			// Cada proceso remoto abre una conexión independiente, por lo que
			// atenderla en paralelo evita bloquear la recepción de las demás.
			go handleConnection(
				conn,
				barrierChan,
				receivedMap,
				mu,
				n,
			)
		}
	}
}

// sendNotification envía un mensaje a otro proceso remoto para avisarle de
// que este proceso ha alcanzado la barrera. Devuelve true si la conexión y el
// envío del mensaje han sido exitosos.
func sendNotification(endpoint string, id int) bool {
	conn, err := net.Dial("tcp", endpoint)

	if err != nil {
		return false
	}

	defer conn.Close()

	_, err = conn.Write(
		[]byte(strconv.Itoa(id)),
	)

	return err == nil
}

// notifyProcess avisa a un proceso remoto de que este proceso ha alcanzado la barrera.
// Si el proceso no está escuchando, reintenta la conexión periódicamente.
func notifyProcess(
	endpoint string,
	id int,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for !sendNotification(endpoint, id) {
		fmt.Println("Error connecting to", endpoint)
		time.Sleep(time.Second)
	}
}

// notifyOtherDistributedProcesses avisa a todos los procesos remotos de que este proceso ha alcanzado la barrera.
func notifyOtherDistributedProcesses(
	endPoints []string,
	lineNumber int,
	wg *sync.WaitGroup,
) {
	for i, ep := range endPoints {
		if i+1 != lineNumber {
			wg.Add(1)
			go notifyProcess(
				ep,
				lineNumber,
				wg,
			)
		}
	}
}

func main() {

	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <endpoints_file> <line_number>")
		return
	}

	endPoints, lineNumber, err := getEndpoints()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	localEndpoint := endPoints[lineNumber-1]

	listener, err := net.Listen("tcp", localEndpoint)
	if err != nil {
		fmt.Println("Error creating listener:", err)
		return
	}

	fmt.Println("Listening on", localEndpoint)

	var mu sync.Mutex
	var notifyWG sync.WaitGroup

	quitChannel := make(chan bool)
	receivedMap := make(map[string]bool)
	barrierChan := make(chan bool)

	go acceptAndHandleConnections(
		listener,
		quitChannel,
		barrierChan,
		&receivedMap,
		&mu,
		len(endPoints),
	)

	notifyOtherDistributedProcesses(
		endPoints,
		lineNumber,
		&notifyWG,
	)

	fmt.Println("Waiting for all the processes to reach the barrier")

	<-barrierChan

	notifyWG.Wait()

	fmt.Println("End Barrier")

	listener.Close()
}
