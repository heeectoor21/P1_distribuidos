package main

//HOLAAA

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// implementacion de barrera
// cada proceso es cliente y servidor
// un proceso notifica a n - 1 procesos que ha alcanzado la barrera
// para pasar la barrera un proceso tiene que recibir n - 1 notificaciones

// TCP/IP para comunicar los distintos procesos
// Dos canales internos para poder comunicar las goroutines (en un mismo proceso)
// Un semáforo para la sección crítica entre las gouroutines (en un mismo proceso)

// lee fichero de direcciones linea a linea
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

// se tiene n - 1 goroutines handleConection() -> se necesita un semaforo mutex para la exclusión mutua
// seccion critica -> contador de notificaciones
func handleConnection(conn net.Conn, barrierChan chan<- bool, received *map[string]bool, mu *sync.Mutex, n int) {
	defer conn.Close()
	buf := make([]byte, 1024)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return
	}
	msg := string(buf)
	mu.Lock()												// IMPORTANTE
	(*received)[msg] = true									// SECCIÓN CRÍTICA
	fmt.Println("Received ", len(*received), " elements")	
	if len(*received) == n-1 {
		barrierChan <- true
	}
	mu.Unlock()
}

// Get enpoints (IP adresse:port for each distributed process)
// Prepara los datos
// Ejecución: go run barrier.go endpoints.txt (fichero con direcciones) id (identificador del proceso)
func getEndpoints() ([]string, int, error) {
    endpointsFile := os.Args[1]
    var endpoints []string  // Por qué esta declaración ?
	lineNumber, err := strconv.Atoi(os.Args[2])
	if err != nil || lineNumber < 1 {
		fmt.Println("Invalid line number")
	} else if endpoints, err = readEndpoints(endpointsFile); err != nil {
		    fmt.Println("Error reading endpoints:", err)
	    } else if lineNumber > len(endpoints) {
		        fmt.Printf("Line number %d out of range\n", lineNumber)
		        err = errors.New("Line number out of range")
	    }
    }

    return endpoints,lineNumber, err
}

// Función de servidor
// Para tratar la notificación de un proceso
func acceptAndHandleConnections(listener net.Listener, quitChannel chan bool, barrierChan chan bool, receivedMap *map[string]bool, mu *sync.Mutex)
 {
	for {
		select {
		case <-quitChannel:
			fmt.Println("Stopping the listener...")
			break
		default:
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				continue
			}
			go handleConnection(conn, barrierChan, &receivedMap, &mu, n)
		}
	}
}

// Para notificar a los n - 1 procesos
// Se tiene n - 1 goroutines para no notificar de forma secuencial
func notifyOtherDistributedProcesses(endPoints [] string, lineNumber int) {
	for i, ep := range endpoints {
		if i+1 != lineNumber {
			go func(ep string) {
				for {
					conn, err := net.Dial("tcp", ep)
					if err != nil {
						fmt.Println("Error connecting to", ep, ":", err)
						time.Sleep(1 * time.Second)
						continue
					}
					_, err = conn.Write([]byte(strconv.Itoa(lineNumber)))
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

	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <endpoints_file> <line_number>")
	} else if endPoints, lineNumber, err := getEndpoints(); err != nil {
        // Get the endpoint for current process
        localEndpoint := endPoints[lineNumber-1]
	    if listener, err = net.Listen("tcp", localEndpoint); err != nil {
		    fmt.Println("Error creating listener:", err)
		} else {
            fmt.Println("Listening on", localEndpoint)

    // Barrier synchronization
	var mu sync.Mutex							// Crea el mutex (semáforo)
	quitChannel := make(chan bool)				// Canal para terminar
	receivedMap := make(map[string]bool)		// Guardar que procesos han notificado 
	barrierChan := make(chan bool)				// Canal para controlar la barrera

	// Para recibir las notificaciones de los n - 1 procesos
    go acceptAndHandleConnections(listener, quitChannel, barrierChan, &receivedMap, &mu)

	// Para notificar a los n - 1 procesos
    notifyOtherDistributedProcesses(endPoints, lineNumber)

	fmt.Println("Waiting for all the processes to reach the barrier")

	listener.Close()
}