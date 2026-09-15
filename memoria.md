
T = t(xon) + t(exec) + t(overhead)
t(exec) = 1s --> mu = K(cores)/t(exec)

synchronous channel


| Escenario | Protocolo | Resultado del Dial/envío | Tiempo |
|---|---|---|---|
| Cliente conecta antes de arrancar el servidor | TCP | Fallo | 315.024 µs |
| Cliente conecta con el servidor ya arrancado | TCP | Éxito | 54.655 µs |
| Cliente envía una letra y recibe la respuesta | UDP | Éxito | 326.383 µs |

