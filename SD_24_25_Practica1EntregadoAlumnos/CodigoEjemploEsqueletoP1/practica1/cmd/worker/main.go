package main

import (
	"encoding/gob"
	"log"
	"net"
	"os"
	"practica1/com"
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

// función processRequest
//

func processRequest(conn net.Conn) {

	// recibe la petición del cliente

	var request com.Request
	decoder := gob.NewDecoder(conn)
	err := decoder.Decode(&request)
	com.CheckError(err)

	// encuentra los primos en el intervalo dado

	primes := findPrimes(request.Interval)

	// envía la respuesta

	reply := com.Reply{
		Id:     request.Id,
		Primes: primes,
	}

	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(&reply)
	com.CheckError(err)
}

func main() {

	// comprueba el número de argumentos
	// argumento 1: dirección ip:puerto

	args := os.Args
	if len(args) != 2 {
		log.Println("Error: endpoint missing: go run worker.go ip:port")
		os.Exit(1)
	}
	endpoint := args[1]

	// crea listener

	listener, err := net.Listen("tcp", endpoint)
	com.CheckError(err)

	log.SetFlags(log.Lshortfile | log.Lmicroseconds)             // comprueba si hay un error
	log.Println("***** Worker listening in endpoint ", endpoint) // configura como se muestra los log

	// si se acepta una conexión se lanza el proceso para atender
	// no hace falta Gourutines porque la concurrencia ya se tiene entre los distintos esclavos

	for {
		conn, err := listener.Accept()
		com.CheckError(err)
		processRequest(conn)
		conn.Close()
	}
}
