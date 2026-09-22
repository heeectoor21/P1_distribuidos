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

// readEndpoints lee del fichero las direcciones de todos los procesos de la
// barrera. Cada línea contiene un endpoint en formato host:puerto.
func readEndpoints(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var endpoints []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			endpoints = append(endpoints, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return endpoints, nil
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
		fmt.Println("Error reading from connection:", err)
		return
	}
	msg := string(buf[:bytesRead])
	mu.Lock()
	(*received)[msg] = true
	fmt.Println("Received", len(*received), "elements")
	// Este proceso ya ha llegado localmente; por tanto, solo debe recibir
	// mensajes de los otros n-1 procesos para poder liberar la barrera.
	if len(*received) == n-1 {
		barrierChan <- true
	}
	mu.Unlock()
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

// notifyOtherDistributedProcesses avisa a todos los procesos, excepto al
// actual, de que este proceso ha alcanzado la barrera. Si un proceso todavía
// no está escuchando, reintenta la conexión periódicamente.
func notifyOtherDistributedProcesses(
	endPoints []string,
	lineNumber int,
) {
	for i, ep := range endPoints {
		if i+1 != lineNumber {
			go func(ep string) {
				for {
					// El listener del proceso remoto puede arrancar después que
					// este proceso; por eso la conexión se reintenta hasta tener éxito.
					conn, err := net.Dial("tcp", ep)
					if err != nil {
						fmt.Println("Error connecting to", ep, ":", err)
						time.Sleep(1 * time.Second)
						continue
					}
					_, err = conn.Write(
						[]byte(strconv.Itoa(lineNumber)),
					)
					if err != nil {
						fmt.Println("Error sending message:", err)
						conn.Close()
						continue
					}
					conn.Close()
					break
				}
			}(ep)
		}
	}
}

func main() {
	var listener net.Listener
	// Se necesitan el fichero de endpoints y el número de línea del proceso.
	if len(os.Args) != 3 {
		fmt.Println(
			"Usage: go run main.go <endpoints_file> <line_number>",
		)
		return
	}
	endPoints, lineNumber, err := getEndpoints()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	localEndpoint := endPoints[lineNumber-1]
	listener, err = net.Listen("tcp", localEndpoint)
	if err != nil {
		fmt.Println("Error creating listener:", err)
		return
	}
	fmt.Println("Listening on", localEndpoint)
	var mu sync.Mutex
	quitChannel := make(chan bool)
	receivedMap := make(map[string]bool)
	barrierChan := make(chan bool)
	// El listener queda atendiendo conexiones mientras el proceso principal
	// notifica su llegada y espera a que lleguen los demás procesos.
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
	)
	fmt.Println(
		"Waiting for all the processes to reach the barrier",
	)
	// La recepción del mensaje en barrierChan indica que han llegado todos
	// los procesos remotos esperados.
	<-barrierChan
	fmt.Println("End Barrier")
	listener.Close()
}
