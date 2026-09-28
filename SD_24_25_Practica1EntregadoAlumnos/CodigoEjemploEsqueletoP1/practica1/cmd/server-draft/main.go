/*
* AUTOR: Rafael Tolosana Calasanz y Unai Arronategui
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2022
* FICHERO: server-draft.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes a la práctica 1
 */
package main

import (
	"encoding/gob"
	"log"
	"net"
	"os"
	"practica1/com"
	"strconv"
)

// PRE: verdad = !foundDivisor
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func isPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// intervalo [interval.A, interval.B]
func findPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.Min; i <= interval.Max; i++ {
		if isPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

// funcion processRequest
// recibe como parámetro la conexión con el cliente

func processRequest(conn net.Conn) {

	// recibe la petición

	var request com.Request
	decoder := gob.NewDecoder(conn)
	err := decoder.Decode(&request)
	com.CheckError(err)

	// encuentra los primos en el intervalo dado

	primes := findPrimes(request.Interval)

	// responde a la petición

	reply := com.Reply{Id: request.Id, Primes: primes}
	encoder := gob.NewEncoder(conn)
	encoder.Encode(&reply)
}

// función worker
// recibe como parámetro el canal tasks para poder recibir la conexión con el cliente
// enviada por el proceso main

func worker(tasks chan net.Conn) {
	for conn := range tasks {
		processRequest(conn)
		conn.Close()
	}
}

func main() {

	// comprueba el número de argumentos
	// argumento 1: dirección ip:puerto
	// argumento 2: número de workers

	args := os.Args
	if len(args) != 3 {
		log.Println("Error: endpoint missing: go run server.go ip:port nWorkers")
		os.Exit(1)
	}
	endpoint := args[1]
	nWorkers, err := strconv.Atoi(os.Args[2])

	// crea listener

	listener, err := net.Listen("tcp", endpoint)

	com.CheckError(err)                              // comprueba si hay un error
	log.SetFlags(log.Lshortfile | log.Lmicroseconds) // configura como se muestra los log

	log.Println("***** Listening for new connection in endpoint ", endpoint)

	// crea canal tasks
	// el canal tasks se usa para envíar la conexión con el cliente desde el
	// proceso main a las Gourutines worker

	tasks := make(chan net.Conn)

	// se lanzan 4 Gourutines worker

	for i := 0; i < nWorkers; i++ {
		go worker(tasks)
	}

	// si se acepta una conexión se envía por el canal tasks a una Gourutine worker
	// la recibira el primero que este disponible

	for {
		conn, err := listener.Accept()
		com.CheckError(err)
		tasks <- conn
	}
}
